#####################################################################
# ECS Celery Autoscaler Policy
#
# Attached to the client's own publisher-app and Celery-worker IAM roles
#####################################################################

data "aws_caller_identity" "current" {}

resource "aws_iam_policy" "ecs_celery_autoscaler" {
  name        = "ecs-celery-autoscaler-policy"
  description = "Allows describing/updating desiredCount on the ECS service and managing task scale-in protection for event-driven autoscaling."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecs:UpdateService", "ecs:DescribeServices"]
        Resource = data.aws_ecs_service.existing.arn
      },
      {
        Effect   = "Allow"
        Action   = ["ecs:GetTaskProtection", "ecs:UpdateTaskProtection"]
        Resource = "arn:aws:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:task/${var.ecs_cluster_name}/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_celery_autoscaler" {
  role       = var.ecs_task_role
  policy_arn = aws_iam_policy.ecs_celery_autoscaler.arn
}