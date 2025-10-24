import json
import boto3
from functools import cached_property
from .spark_env import get_dbutils

dbutils = get_dbutils()

class SecretManagerClient:
    def __init__(self, credential_name: str, region: str = "eu-west-1"):
        self._credential_name = credential_name
        self._region = region

    @cached_property
    def boto3_session(self) -> boto3.Session:
        return boto3.Session(
            botocore_session=dbutils.credentials.getServiceCredentialsProvider(self._credential_name),
            region_name=self._region
        )

    @cached_property
    def client(self):
        return self.boto3_session.client("secretsmanager")

    def get_secret(self, secret_name: str) -> dict:
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            return json.loads(response["SecretString"])
        except self.client.exceptions.ResourceNotFoundException:
            raise ValueError(f"Secret '{secret_name}' not found.")
        except self.client.exceptions.ClientError as e:
            raise RuntimeError(f"Error retrieving secret '{secret_name}': {e}")
        except KeyError:
            raise ValueError(f"Secret '{secret_name}' does not contain 'SecretString'.")
        except json.JSONDecodeError:
            raise ValueError(f"Secret '{secret_name}' is not valid JSON.")