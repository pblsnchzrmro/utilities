resource "aws_sns_topic" "sns_topic_with_subscription" {
  name   = var.sns_topic_name
  policy = var.sns_policy
  tags   = merge(var.tags, { Name = var.sns_topic_name })
}

resource "aws_sns_topic_policy" "sns_topic_policy" {
  arn = aws_sns_topic.sns_topic_with_subscription.arn

  policy = <<EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "CodePipeline_publish",
        "Effect": "Allow",
        "Principal": {
            "Service": "codepipeline.amazonaws.com"
        },
        "Action": "SNS:Publish",
        "Resource": "${aws_sns_topic.sns_topic_with_subscription.arn}"
      },
      {
        "Sid": "Notification_publish",
        "Effect": "Allow",
        "Principal": {
          "Service": "codestar-notifications.amazonaws.com"
        },
        "Action": "SNS:Publish",
        "Resource": "${aws_sns_topic.sns_topic_with_subscription.arn}"
      },
      {
        "Sid": "Notification_event_publish",
        "Effect": "Allow",
        "Principal": {
          "Service": "events.amazonaws.com"
        },
        "Action": "SNS:Publish",
        "Resource": "${aws_sns_topic.sns_topic_with_subscription.arn}"
      }
    ]
  }

  EOF
}
