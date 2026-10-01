variable "aws_region" {
    type        = string
    default     = "us-east-1"
}

variable "environment" {
    type        = string
    default     = "prod"
}

variable "project_name" {
    type        = string
    default     = "paysim-pipeline"
}

variable "raw_bucket_name" {
    type        = string
    default     = "paysim-pipeline-raw-amonmugo"
}

variable "curated_bucket_name" {"
    type        = string
    default     = "paysim-pipeline-curated-amonmugo"
}

variable "ecr_repository_name" {
    type        = string
    default     = "paysim-pyspark"
}

variable "emr_application_name" {
    type        = string
    default     = "paysim-emr"
}

variable "emr_image_tag" {
    type        = string
    default     = "v5"
}

variable "emr_execution_role_name" {
    type        = string
    default     = "paysim-emr-serverless-execution"
}

variable "ingestion_name" {
    type        = string
    default     = "paysim-ingestion"
}

variable "glue_database_name" {
    type        = string
    default     = "paysim_fraud_db"
}

variable "tfstate_bucket_name" {
    type        = string
    default     = "paysim-tfstate-amonmugo"
}

variable "tfstate_lock_table_name" {
    type        = string
    default     = "paysim-tfstate-lock"
}