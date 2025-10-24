output "sns_topic_name" {
  value = var.sns_topic_name
}

output "sns_topic_arn" {
  value = join("", aws_sns_topic.sns_topic_with_subscription.*.arn)
}