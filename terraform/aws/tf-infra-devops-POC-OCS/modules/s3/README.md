# S3 Terraform Module

This Terraform module provisions an Amazon S3 bucket with optional lifecycle rules, versioning, and public access blocking settings.

## Resources Created
This module creates the following resources:
- **AWS S3 Bucket**
- **AWS S3 Bucket Lifecycle Rules** (if provided)
- **AWS S3 Bucket Versioning** (optional, default: Suspended)
- **AWS S3 Bucket Public Access Block** (default: block)

## Usage
To use this module, include the following in your Terraform configuration:

```hcl
module "s3" {
  source  = "./modules/s3"
  bucket  = "thisisatest"
  versioning = "Enabled"
  lifecycle_rule = [
    {
      id      = "test1"
      prefix  = "test1/"
      transition = [
        {
          days          = 30
          storage_class = "ONEZONE_IA"
        },
        {
          days          = 90
          storage_class = "GLACIER"
        }
      ]
      expiration = 365
    },
    {
      id      = "test2"
      prefix  = "test2/"
      transition = [
        {
          days          = 30
          storage_class = "DEEP_ARCHIVE"
        }
      ]
    }
  ]
  tags = { Environment = "dev" }
}
```

## Inputs

| Name                  | Type   | Default     | Required | Description |
|-----------------------|--------|-------------|----------|-------------|
| `bucket`             | string | n/a         | Yes      | The name of the S3 bucket. |
| `versioning`         | string | "Suspended" | No       | The versioning status. Allowed values: `Enabled`, `Suspended`, `Disabled`. |
| `lifecycle_rule`     | list   | `[]`        | No       | A list of lifecycle rules for object transitions and expirations. |
| `tags`               | map(any) | `{}`       | No       | Tags to apply to the S3 bucket. |

## Outputs

| Name                           | Description |
|--------------------------------|-------------|
| `id`                          | The ID of the created S3 bucket. |
| `arn`                         | The ARN of the created S3 bucket. |
| `bucket_regional_domain_name` | The regional domain name of the bucket. |

## Notes
- Lifecycle rules allow automatic transitions of objects to different storage classes and expiration of objects.
- Public access to the bucket is blocked by default.
- Versioning is disabled by default but can be set to `Enabled` or `Suspended`.

## Importing an Existing S3 Bucket
To import an existing S3 bucket into Terraform state, run:
```sh
tf import aws_s3_bucket.example_bucket bucket-name
```

