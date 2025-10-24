# common inputs
variable "bucket" {}
variable "lifecycle_rule" {
  default = []
}
variable "versioning" {
  default = "Suspended"
}

variable "block_public_acls" {
  default = true
}

variable "block_public_policy" {
  default = true
}

variable "ignore_public_acls" {
  default = true
}

variable "restrict_public_buckets" {
  default = true
}

variable "tags" {
  type = map(any)
}