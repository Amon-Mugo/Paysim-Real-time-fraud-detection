#you know it the stotage

resource "aws_s3_bucket" "paysim_fraud_pipeline_raw" {
  bucket = var.raw_bucket_name

  tags = {
    Project     = var.project_name
    Layer       = "raw"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_ownership_controls" "paysim_fraud_pipeline_raw" {
  bucket = aws_s3_bucket.paysim_fraud_pipeline_raw.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "paysim_fraud_pipeline_raw" {
  bucket                  = aws_s3_bucket.paysim_fraud_pipeline_raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Raw data has no self-versioning mechanism of its own (unlike Iceberg

resource "aws_s3_bucket_versioning" "paysim_fraud_pipeline_raw" {
  bucket = aws_s3_bucket.paysim_fraud_pipeline_raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "paysim_fraud_pipeline_raw" {
  bucket = aws_s3_bucket.paysim_fraud_pipeline_raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "paysim_fraud_pipeline_raw" {
  depends_on = [aws_s3_bucket_versioning.paysim_fraud_pipeline_raw]
  bucket     = aws_s3_bucket.paysim_fraud_pipeline_raw.id

  rule {
    id     = "raw-data-cleanup"
    status = "Enabled"
    filter {}

    transition {
      days          = 40
      storage_class = "INTELLIGENT_TIERING"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 6
    }
  }
}

resource "aws_s3_bucket" "paysim_fraud_pipeline_curated" {
  bucket = var.curated_bucket_name

  tags = {
    Project     = var.project_name
    Layer       = "curated"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_ownership_controls" "paysim_fraud_pipeline_curated" {
  bucket = aws_s3_bucket.paysim_fraud_pipeline_curated.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "paysim_fraud_pipeline_curated" {
  bucket                  = aws_s3_bucket.paysim_fraud_pipeline_curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


resource "aws_s3_bucket_versioning" "paysim_fraud_pipeline_curated" {
  bucket = aws_s3_bucket.paysim_fraud_pipeline_curated.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "paysim_fraud_pipeline_curated" {
  bucket = aws_s3_bucket.paysim_fraud_pipeline_curated.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "paysim_fraud_pipeline_curated" {
  depends_on = [aws_s3_bucket_versioning.paysim_fraud_pipeline_curated]
  bucket     = aws_s3_bucket.paysim_fraud_pipeline_curated.id

  rule {
    id     = "cleanup-curated-data"
    status = "Enabled"

    filter {
      prefix = "iceberg/warehouse/"
    }

    transition {
      days          = 40
      storage_class = "INTELLIGENT_TIERING"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 6
    }
  }
  # so they expire fast rather than transitioning to cheaper storage.
  rule {
    id     = "expire-stale-checkpoints"
    status = "Enabled"

    filter {
      prefix = "checkpoints/"
    }

    expiration {
      days = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

