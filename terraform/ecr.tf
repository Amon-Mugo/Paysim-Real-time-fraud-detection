# ECR repository for the Spark/EMR Serverless custom runtime imag

resource "aws_ecr_repository" "paysim_fraud_pipeline_ecr" {
  name                 = var.ecr_repository_name
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
    Purpose   = "ecr_repository"
    ManagedBy = "terraform"
  }
}

resource "aws_ecr_lifecycle_policy" "paysim_fraud_pipeline_ecr" {
  repository = aws_ecr_repository.paysim_fraud_pipeline_ecr.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images older than 7 days"
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

# Allows EMR Serverless to pull images at runtime.
resource "aws_ecr_repository_policy" "paysim_fraud_pipeline_ecr" {
  repository = aws_ecr_repository.paysim_fraud_pipeline_ecr.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowEmrServerlessAccess"
        Effect = "Allow"
        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }
        Action = [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchCheckLayerAvailability",
          "ecr:DescribeImages",
        ]
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
          ArnLike = {
            "aws:SourceArn" = "arn:aws:emr-serverless:${var.aws_region}:${data.aws_caller_identity.current.account_id}:/applications/${var.emr_application_id}"
          }
        }
      }
    ]
  })
}