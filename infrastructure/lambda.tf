# Lambda that starts the Glue ETL job when new objects land under raw/transactions/.

data "archive_file" "lambda_trigger_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda/trigger.py"
  output_path = "${path.module}/.build/lambda_trigger.zip"
}

resource "aws_lambda_function" "glue_trigger" {
  function_name = "${var.project_name}-trigger"
  role          = aws_iam_role.lambda_trigger.arn
  handler       = "trigger.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60
  memory_size   = 256

  filename         = data.archive_file.lambda_trigger_zip.output_path
  source_code_hash = data.archive_file.lambda_trigger_zip.output_base64sha256

  environment {
    variables = {
      GLUE_JOB_NAME         = aws_glue_job.fraud_etl.name
      CURATED_OUTPUT_PREFIX = "curated/transactions"
      OUTPUT_BUCKET         = aws_s3_bucket.datalake.bucket
      LOG_LEVEL             = "INFO"
    }
  }

  tags = local.common_tags
}

resource "aws_lambda_permission" "allow_s3_invoke_trigger" {
  statement_id  = "AllowS3InvokeGlueTrigger"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.glue_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.datalake.arn
}

resource "aws_s3_bucket_notification" "raw_to_lambda" {
  bucket = aws_s3_bucket.datalake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.glue_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/transactions/"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke_trigger]
}
