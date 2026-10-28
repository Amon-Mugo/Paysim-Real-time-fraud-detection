# Used in establishing a secure IAM policy for ECS to stream data of the PaySim
# Reuse the msk_cluster_arn and msk_topic_arn already in iam.tf
# Read the csv path, write the ground truth and producer of MSK

resource "aws_iam_role" "paysim_fraud_pipeline_generator_task" {
  name = "${var.project_name}-generator-task"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
  tags = {
    Project = var.project_name
    Purpose = "generator task role"
  }
}

# Used in granting permission to read csv and write ground truth to ground-truth
resource "aws_iam_policy" "paysim_fraud_pipeline_generator_s3_access" {
  name        = "paysim-fraud-pipeline-generator-s3-access-policy"
  description = "Allow generator to read PaySim CSV and write ground truth in the raw bucket"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3:GetObject"
      Resource = "${aws_s3_bucket.paysim_fraud_pipeline_raw.arn}/paysim/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.paysim_fraud_pipeline_raw.arn}/ground-truth/*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "paysim_fraud_pipeline_generator_s3_access" {
  role       = aws_iam_role.paysim_fraud_pipeline_generator_task.name
  policy_arn = aws_iam_policy.paysim_fraud_pipeline_generator_s3_access.arn
}

# MSK Serverless authentication IAM for producing transaction events
resource "aws_iam_policy" "paysim_fraud_pipeline_generator_msk_access" {
  name        = "paysim-fraud-pipeline-generator-msk-access-policy"
  description = "Allows the generator to connect to MSK and write to topics via IAM auth"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "kafka-cluster:Connect",        # Establish connection 
        "kafka-cluster:DescribeCluster" # Read the cluster data
      ]
      Resource = local.msk_cluster_arn
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:DescribeTopic", # Enable inspect and query kafka data
          "kafka-cluster:WriteData"      # Enable to produce and write message into kafka topic
        ]
        Resource = local.msk_topic_arn
    }]
  })
}

resource "aws_iam_role_policy_attachment" "paysim_fraud_pipeline_generator_msk_access" {
  role       = aws_iam_role.paysim_fraud_pipeline_generator_task.name
  policy_arn = aws_iam_policy.paysim_fraud_pipeline_generator_msk_access.arn
}

# Enable Fargate to pull images and write container logs
resource "aws_iam_role" "paysim_fraud_pipeline_generator_execution" {
  name = "${var.project_name}-generator-execution"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = {
    Project = var.project_name
    Purpose = "Generator Fargate execution role"
  }
}

resource "aws_iam_role_policy_attachment" "paysim_fraud_pipeline_execution" {
  role       = aws_iam_role.paysim_fraud_pipeline_generator_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}