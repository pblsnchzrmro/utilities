# SNS Terraform Module

This Terraform module provisions an Amazon SNS topic with an optional policy to allow specific AWS services (CodePipeline, CodeStar Notifications, and EventBridge) to publish messages to it.

## Resources Created

This module creates the following resources:

- **AWS SNS Topic**
- **AWS SNS Topic Policy** (optional, based on provided `sns_policy` variable)

## Usage

To use this module, include the following in your Terraform configuration:

```hcl
module "sns" {
  source         = "./modules/sns"
  sns_topic_name = "${var.application_id}-nametopic-${random_string.s3_suffix.result}"
  tags          = { Environment = "dev" }
}
```

## Inputs

| Name             | Type     | Default | Description                                                                                                                         |
| ---------------- | -------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `sns_topic_name` | string   | n/a     | The name of the SNS topic.                                                                                                          |
| `sns_policy`     | string   | ""      | (Optional) A custom SNS topic policy in JSON format. If not provided, a default policy is created to allow AWS services to publish. |
| `tags`           | map(any) | `{}`    | (Optional) Tags to apply to the SNS topic.                                                                                          |

## Outputs

| Name             | Description                        |
| ---------------- | ---------------------------------- |
| `sns_topic_name` | The name of the created SNS topic. |
| `sns_topic_arn`  | The ARN of the created SNS topic.  |

## Notes

- Subscriptions to the SNS topic must be managed separately after applying Terraform.
- To unsubscribe from the SNS topic, use the AWS console or run `terraform destroy` followed by `terraform apply`.

