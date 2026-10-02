#glue data warehouse used by iceberg catalog

resource "aws_glue_catalog_database" "paysim_fraud_pipeline" {
  name         = var.glue_database_name
  description  = "Iceberg catalog database for the PaySim fraud pipeline curated tables"
  location_uri = "s3://${aws_s3_bucket.paysim_fraud_pipeline_curated.bucket}/warehouse/"

  tags = {
    Project = var.project_name
    Purpose = "Iceberg catalog database"
  }
}