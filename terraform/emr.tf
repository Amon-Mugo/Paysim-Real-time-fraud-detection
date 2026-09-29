
resource "aws_emrserverless_application" "paysim_fraud_pipeline_emr" {
  name          = var.emr_application_name
  release_label = "emr-7.10"
  type          = "Spark"

  image_configuration {
    image_uri = "${aws_ecr_repository.paysim_fraud_pipeline_ecr.repository_url}:${var.emr_image_tag}"
  }

  maximum_capacity {
    cpu    = "4 vCPU"
    memory = "16 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  network_configuration {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.msk_emr.id]
  }

  # used for CD pipeline: allow image tag updates outside Terraform
  lifecycle {
    ignore_changes = [image_configuration]
  }

  tags = {
    Project   = var.project_name
    Purpose   = "emr_serverless_application"
    ManagedBy = "terraform"
  }
}