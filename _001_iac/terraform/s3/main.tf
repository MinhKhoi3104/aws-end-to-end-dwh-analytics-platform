# ref: https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket
# ref: https://github.com/rahulwagh/aws-terraform-course-repo
######################################
# Random suffix to avoid name conflict
######################################
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

######################################
# Main S3 bucket
######################################
resource "aws_s3_bucket" "project_bucket" {
  bucket = "${var.bucket_name}-${random_id.bucket_suffix.hex}"

  tags = {
    Name        = var.bucket_name
    Environment = "dev"
  }
}

######################################
# Block all public access 
######################################
resource "aws_s3_bucket_public_access_block" "project_bucket_block_public" {
  bucket = aws_s3_bucket.project_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

######################################
# Enable Versioning
######################################
resource "aws_s3_bucket_versioning" "project_bucket_versioning" {
  bucket = aws_s3_bucket.project_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

######################################
# Lifecycle rules for non-current objects
######################################
resource "aws_s3_bucket_lifecycle_configuration" "project_bucket_lifecycle" {
  bucket = aws_s3_bucket.project_bucket.id

  rule {
    id     = "noncurrent-version-lifecycle"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = 60
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
  rule {
    id     = "redshift-temp-expire"
    status = "Enabled"

    filter {
      prefix = "redshift-temp/"
    }

    expiration {
      days = 1
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  depends_on = [
    aws_s3_bucket_versioning.project_bucket_versioning
  ]
}

######################################
# Log bucket (for S3 access logs)
######################################
resource "aws_s3_bucket" "log_bucket" {
  bucket = "s3-access-logs-${random_id.bucket_suffix.hex}"

  tags = {
    Name        = "s3-access-logs"
    Environment = "dev"
  }
}

resource "aws_s3_bucket_public_access_block" "log_bucket_block_public" {
  bucket = aws_s3_bucket.log_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "project_bucket_logging" {
  bucket = aws_s3_bucket.project_bucket.id

  target_bucket = aws_s3_bucket.log_bucket.id
  target_prefix = "project-bucket/"
}
