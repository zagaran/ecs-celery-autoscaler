#####################################################################
# Data Sources
#####################################################################

data "aws_ecs_cluster" "ecs_cluster" {
  cluster_name = var.ecs_cluster_name
}

data "aws_ecs_service" "existing" {
  service_name = var.ecs_service_name
  cluster_arn  = data.aws_ecs_cluster.ecs_cluster.arn
}
