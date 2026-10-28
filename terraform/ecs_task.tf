# used in the automation of AWS infrastructure needed to run
# paysim data as a serverless container on Amazon ECS using AWS Fargate

resource "aws_cloudwatch_log_group" "paysim_fraud_pipeline_generator" {
  name              = "/ecs/${var.project_name}-generator"
  retention_in_days = 7

  tags = {
    Project   = var.project_name
    Purpose   = "generator_log_group"
    ManagedBy = "terraform"
  }
}

# declare aws ecs task
resource "aws_ecs_task_definition" "paysim_fraud_pipeline_generator" {
  family                   = "${var.project_name}-generator"
  requires_compatibilities = ["FARGATE"]                                                # the compute engine to run AWS Fargate
  network_mode             = "awsvpc"                                                   # enables the fargate to have an ip address and a private network
  cpu                      = 1024                                                       # 1 cpu
  memory                   = 2048                                                       # 2 gb ram
  execution_role_arn       = aws_iam_role.paysim_fraud_pipeline_generator_execution.arn # grants ECS permission to pull container images from amazon ecr and push logs in cloud watch
  task_role_arn            = aws_iam_role.paysim_fraud_pipeline_generator_task.arn      # grants permission inside the container

  container_definitions = jsonencode([
    {
      name  = "generator"
      image = "${aws_ecr_repository.paysim_fraud_pipeline_generator_ecr.repository_url}:${var.generator_image_tag}"
      # specifies the ECR URL and specifies the image tag variable
      essential = true

      logConfiguration = {
        logDriver = "awslogs" # logs them into cloudwatch
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.paysim_fraud_pipeline_generator.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "generator"
        }
      }
    }
  ])

  tags = {
    Project   = var.project_name
    Purpose   = "generator_task_definition"
    ManagedBy = "terraform"
  }
}

output "generator_task_definition_arn" {
  value = aws_ecs_task_definition.paysim_fraud_pipeline_generator.arn
}