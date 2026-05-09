# S3 buckets for the fraud analytics lake and MWAA Dag/code storage prefixes.

locals {
  common_tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}

resource "aws_s3_bucket" "datalake" {
  bucket = "${var.project_name}-${var.your_name}-datalake"

  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "mwaa" {
  bucket = "mwaa-fraud-${var.your_name}"

  tags = local.common_tags
}
