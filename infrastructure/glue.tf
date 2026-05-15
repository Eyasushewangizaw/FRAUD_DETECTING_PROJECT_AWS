# Glue Data Catalog, packaged PySpark ETL script, batch job, and crawler over curated Parquet.

locals {
  glue_script_key  = "glue-scripts/fraud_etl.py"
  glue_temp_prefix = "glue-temporary/"
}

resource "aws_s3_object" "glue_fraud_etl_script" {
  bucket = aws_s3_bucket.datalake.id
  key    = local.glue_script_key
  source = "${path.module}/../src/glue/fraud_etl.py"
  etag   = filemd5("${path.module}/../src/glue/fraud_etl.py")

  tags = local.common_tags
}

resource "aws_glue_catalog_database" "fraud" {
  name        = local.glue_database_name
  description = "Fraud pipeline curated tables (crawler-managed) and job outputs"

  tags = local.common_tags
}

resource "aws_glue_job" "fraud_etl" {
  name              = "${var.project_name}-etl"
  role_arn          = aws_iam_role.glue_etl.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 1
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.datalake.bucket}/${local.glue_script_key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.datalake.bucket}/${local.glue_temp_prefix}"
  }

  tags = local.common_tags
}

resource "aws_glue_crawler" "fraud_curated" {
  name          = "fraud-curated-crawler"
  role          = aws_iam_role.glue_etl.name
  database_name = aws_glue_catalog_database.fraud.name

  s3_target {
    path = "s3://${aws_s3_bucket.datalake.bucket}/curated/transactions/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
  }

  tags = local.common_tags
}
