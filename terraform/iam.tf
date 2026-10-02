# IAM rol

locals {
  msk_cluster_arn   = aws_msk_serverless_cluster.paysim_fraud_pipeline.arn
  msk_topic_arn     = "${replace(local.msk_cluster_arn, ":cluster/", ":topic/")}/*"
  msk_group_arn     = "${replace(local.msk_cluster_arn, ":cluster/", ":group/")}/*"
  glue_database_arn = aws_glue_catalog_database.paysim_fraud_pipeline.arn
  glue_table_arn    = "${replace(local.glue_database_arn, ":database/", ":table/")}/*"
  glue_catalog_arn  = "arn:aws:glue:${element(split(":", local.glue_database_arn), 3)}:${data.aws_caller_identity.current.account_id}:catalog"
}

resource "aws_iam_role" "paysim_fraud_pipeline_ingestion" {
  name = var.ingestion_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
      }
      Action = "sts:AssumeRole"
      Condition = {
        StringLike = {
          "aws:PrincipalArn" = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/aws-reserved/sso.amazonaws.com/*AWSReservedSSO_AdministratorAccess_*"
        }
      }
    }]
  })

  tags = {
    Project = var.project_name
    Purpose = "Ingestion role assumable by SSO administrators"
  }
}

resource "aws_iam_policy" "paysim_fraud_pipeline_ingestion_s3_write" {
  name        = "paysim-fraud-pipeline-ingestion-s3-write-policy"
  description = "Allows writing objects to the raw data bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3:PutObject"
      Resource = "${aws_s3_bucket.paysim_fraud_pipeline_raw.arn}/*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "paysim_fraud_pipeline_ingestion_s3_write" {
  role       = aws_iam_role.paysim_fraud_pipeline_ingestion.name
  policy_arn = aws_iam_policy.paysim_fraud_pipeline_ingestion_s3_write.arn
}

# EMR Serverless execution rol
resource "aws_iam_role" "paysim_fraud_pipeline_emr_execution" {
  name = var.emr_execution_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "emr-serverless.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = {
    Project = var.project_name
    Purpose = "EMR Serverless execution role"
  }
}

# Read raw; read/write/delete curated ,Iceberg warehouse and checkpoints
resource "aws_iam_policy" "paysim_fraud_pipeline_emr_s3_access" {
  name        = "paysim-fraud-pipeline-emr-s3-access-policy"
  description = "Allows EMR Serverless to read the raw bucket and read/write the curated bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.paysim_fraud_pipeline_raw.arn,
          "${aws_s3_bucket.paysim_fraud_pipeline_raw.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [aws_s3_bucket.paysim_fraud_pipeline_curated.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = ["${aws_s3_bucket.paysim_fraud_pipeline_curated.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "paysim_fraud_pipeline_emr_s3_access" {
  role       = aws_iam_role.paysim_fraud_pipeline_emr_execution.name
  policy_arn = aws_iam_policy.paysim_fraud_pipeline_emr_s3_access.arn
}

# MSK Serverless IAM authentication for Spark Structured Streaming.
resource "aws_iam_policy" "paysim_fraud_pipeline_emr_msk_access" {
  name        = "paysim-fraud-pipeline-emr-msk-access-policy"
  description = "Allows EMR Serverless to connect to MSK and read/write topics via IAM auth"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster"
        ]
        Resource = local.msk_cluster_arn
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData"
        ]
        Resource = local.msk_topic_arn
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:DescribeGroup",
          "kafka-cluster:AlterGroup"
        ]
        Resource = local.msk_group_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "paysim_fraud_pipeline_emr_msk_access" {
  role       = aws_iam_role.paysim_fraud_pipeline_emr_execution.name
  policy_arn = aws_iam_policy.paysim_fraud_pipeline_emr_msk_access.arn
}

resource "aws_iam_policy" "paysim_fraud_pipeline_emr_logging" {
  name        = "paysim-fraud-pipeline-emr-logging-policy"
  description = "Allows EMR Serverless to write application logs to CloudWatch Logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ]
      Resource = [
        "arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless*",
        "arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless*:*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "paysim_fraud_pipeline_emr_logging" {
  role       = aws_iam_role.paysim_fraud_pipeline_emr_execution.name
  policy_arn = aws_iam_policy.paysim_fraud_pipeline_emr_logging.arn
}

resource "aws_iam_policy" "paysim_fraud_pipeline_emr_ecr_pull" {
  name        = "paysim-fraud-pipeline-emr-ecr-pull-policy"
  description = "Allows the EMR Serverless execution role to pull the runtime image from ECR"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchCheckLayerAvailability",
          "ecr:DescribeImages"
        ]
        Resource = aws_ecr_repository.paysim_fraud_pipeline_ecr.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "paysim_fraud_pipeline_emr_ecr_pull" {
  role       = aws_iam_role.paysim_fraud_pipeline_emr_execution.name
  policy_arn = aws_iam_policy.paysim_fraud_pipeline_emr_ecr_pull.arn
}

# Glue Data Catalog access for the Iceberg catalog.
resource "aws_iam_policy" "paysim_fraud_pipeline_emr_glue_access" {
  name        = "paysim-fraud-pipeline-emr-glue-access-policy"
  description = "Allows EMR Serverless to manage Iceberg tables in the pipeline's Glue database"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases"
        ]
        Resource = [
          local.glue_catalog_arn,
          local.glue_database_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:CreateTable",
          "glue:DeleteTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdateTable"
        ]
        Resource = [
          local.glue_catalog_arn,
          local.glue_database_arn,
          local.glue_table_arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "paysim_fraud_pipeline_emr_glue_access" {
  role       = aws_iam_role.paysim_fraud_pipeline_emr_execution.name
  policy_arn = aws_iam_policy.paysim_fraud_pipeline_emr_glue_access.arn
}