from .spark_env import get_spark


class RedshiftClient:
    def __init__(self, secret: dict, database: str):
        self._secret = secret
        self._database = database
        self._validate_secret()

    def _validate_secret(self):
        required_keys = {"username", "password", "host"}
        missing = required_keys - self._secret.keys()
        if missing:
            raise ValueError(f"Missing keys in secret: {missing}")

    @property
    def username(self) -> str:
        return self._secret["username"]

    @property
    def password(self) -> str:
        return self._secret["password"]

    @property
    def host(self) -> str:
        return self._secret["host"]

    @property
    def port(self) -> int:
        return int(self._secret.get("port", 5439))

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:redshift://{self.host}:{self.port}/{self._database}"

    def read_table(self, dbtable: str):
        spark = get_spark()
        try:
            return spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", dbtable) \
                .option("user", self.username) \
                .option("password", self.password) \
                .option("driver", "com.amazon.redshift.jdbc.Driver") \
                .load()
        except Exception as e:
            raise RuntimeError(f"Failed to read table '{dbtable}': {e}")

    def read_query(self, sql_query: str):
        spark = get_spark()
        try:
            return spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", sql_query) \
                .option("user", self.username) \
                .option("password", self.password) \
                .option("driver", "com.amazon.redshift.jdbc.Driver") \
                .load()
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {e}")
