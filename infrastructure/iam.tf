#####################################################################
# Celery ECS Autoscaler Policy
#
# Attached to the client's own publisher-app and Celery-worker IAM roles
#####################################################################

resource "aws_iam_policy" "celery_ecs_autoscaler" {
  name        = "celery-ecs-autoscaler-policy"
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

resource "aws_iam_role_policy_attachment" "celery_ecs_autoscaler" {
  role       = var.ecs_task_role
  policy_arn = aws_iam_policy.celery_ecs_autoscaler.arn
}