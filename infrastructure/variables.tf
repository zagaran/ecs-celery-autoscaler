variable "aws_region" {
  type     = string
  nullable = false
}

variable "ecs_cluster_name" {
  type     = string
  nullable = false
}

variable "ecs_service_name" {
  type        = string
  nullable    = false
}

variable "ecs_task_role" {
  type        = string
  nullable    = false
  description = "Name of the IAM role to attach the ecs:UpdateService/DescribeServices policy to."
}