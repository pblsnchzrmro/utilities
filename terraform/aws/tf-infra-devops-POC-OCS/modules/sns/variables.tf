variable "sns_topic_name" {
  type = string
}

variable "sns_policy" {
  type    = string
  default = ""
}

variable "tags" {
  type    = map(any)
  default = {}
}