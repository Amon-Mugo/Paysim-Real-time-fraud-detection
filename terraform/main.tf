terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Phase 2: uncomment after the backend-bootstrap resources exist, then run
  # `terraform init -migrate-state` with the -backend-config flags below.
  # Partial configuration: bucket and dynamodb_table are supplied at init time.
  #
  backend "s3" {
    key     = "terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}