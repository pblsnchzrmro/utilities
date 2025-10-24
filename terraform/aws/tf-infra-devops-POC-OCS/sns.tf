### AWS SNS Topics

# # Topic aprobadores PRE-PRODUCCION
# module "snowflake-cicd-pipeline-approvers-pre" {
#   source         = "./modules/sns"
#   sns_topic_name = "snowflake-cicd-pipeline-approvers-pre"
#   tags           = local.tags
# }


# Topic aprobadores PRODUCCION - notificaciones para confirmar el paso del plan al apply
module "snowflake-cicd-pipeline-approvers-pro" {
  source         = "./modules/sns"
  sns_topic_name = "snowflake-cicd-pipeline-approvers-pro"
  tags           = local.tags
}

# Emails a notificar para las pipelines de PRO
resource "aws_sns_topic_subscription" "email-approvers-pro" {
  topic_arn = module.snowflake-cicd-pipeline-approvers-pro.sns_topic_arn
  protocol  = "email"
  endpoint  = "grupobigdata@mediaset.es"
}

#######################################################
# Topic para notificaciones de error - cualquier fallo de ejecución será notificado aqui
module "snowflake-cicd-pipeline-error-dev" {
  source         = "./modules/sns"
  sns_topic_name = "snowflake-cicd-pipeline-error-dev"
  tags           = local.tags
}

module "snowflake-cicd-pipeline-error-pre" {
  source         = "./modules/sns"
  sns_topic_name = "snowflake-cicd-pipeline-error-pre"
  tags           = local.tags
}

module "snowflake-cicd-pipeline-error-pro" {
  source         = "./modules/sns"
  sns_topic_name = "snowflake-cicd-pipeline-error-pro"
  tags           = local.tags
}

# # Emails a notificar por un error en la ejecución de una pipeline
# resource "aws_sns_topic_subscription" "email-pipeline-error-dev" {
#   topic_arn = module.snowflake-cicd-pipeline-error-dev.sns_topic_arn
#   protocol  = "email"
#   endpoint  = "grupobigdata@mediaset.es"
# }

# resource "aws_sns_topic_subscription" "email-pipeline-error-pre" {
#   topic_arn = module.snowflake-cicd-pipeline-error-pre.sns_topic_arn
#   protocol  = "email"
#   endpoint  = "grupobigdata@mediaset.es"
# }

# resource "aws_sns_topic_subscription" "email-pipeline-error-pro" {
#   topic_arn = module.snowflake-cicd-pipeline-error-pro.sns_topic_arn
#   protocol  = "email"
#   endpoint  = "grupobigdata@mediaset.es"
# }
