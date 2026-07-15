#####################################################################
# ECS Celery Autoscaler Policy
#
# Attached to the client's own publisher-app and Celery-worker IAM roles
#####################################################################

resource "aws_iam_policy" "ecs_celery_autoscaler" {
  name        = "ecs-celery-autoscaler-policy"
  description = "Allows describing/updating desiredCount on the ECS service for event-driven autoscaling."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ecs:UpdateService", "ecs:DescribeServices"]
      Resource = data.aws_ecs_service.existing.arn
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_celery_autoscaler" {
  role       = var.ecs_task_role
  policy_arn = aws_iam_policy.ecs_celery_autoscaler.arn
}