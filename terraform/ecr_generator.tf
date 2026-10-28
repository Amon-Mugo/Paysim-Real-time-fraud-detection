# used for ecs fargate to enable building images in docker for kafka data streams

resource "aws_ecr_repository" "paysim_fraud_pipeline_generator_ecr" {
  name                 = var.generator_ecr_repository_name
  force_delete         = true
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Project   = var.project_name
    Purpose   = "generator_ecr_repository"
    ManagedBy = "terraform"
  }
}

resource "aws_ecr_lifecycle_policy" "paysim_fraud_pipeline_generator_ecr" {
  repository = aws_ecr_repository.paysim_fraud_pipeline_generator_ecr.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Keep last 7 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 7
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# used in ci/cd thus enabling docker pull images 
output "generator_ecr_repository" {
  value = aws_ecr_repository.paysim_fraud_pipeline_generator_ecr.repository_url
}