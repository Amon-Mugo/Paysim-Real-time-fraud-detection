#cluster for ecs that is used in running paysim generator as fargate task

resource "aws_ecs_cluster" "paysim_fraud_pipeline_generator" {
  name = "${var.project_name}-generator"
  tags = {
    Project   = var.project_name
    Purpose   = "generator_ecs_cluster"
    ManagedBy = "terraform"
  }
}

output "generator_ecs_cluster_name" {
  value = aws_ecs_cluster.paysim_fraud_pipeline_generator.name
}