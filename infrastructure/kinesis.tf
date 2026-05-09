# Kinesis Data Stream plus Firehose -> S3 raw landing for fraud transaction events.

resource "aws_cloudwatch_log_group" "transactions_firehose" {
  name              = "/aws/kinesisfirehose/${var.project_name}-transactions-firehose"
  retention_in_days = 7

  tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}

resource "aws_cloudwatch_log_stream" "transactions_firehose_s3" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.transactions_firehose.name
}

resource "aws_iam_role" "transactions_firehose" {
  name = "${var.project_name}-transactions-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}

resource "aws_iam_role_policy" "transactions_firehose" {
  name = "${var.project_name}-transactions-firehose"
  role = aws_iam_role.transactions_firehose.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadKinesisSource"
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards",
        ]
        Resource = aws_kinesis_stream.transactions.arn
      },
      {
        Sid    = "WriteS3Dest"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject",
        ]
        Resource = [
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*",
        ]
      },
      {
        Sid    = "CWLogsDeliveryCreate"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
        ]
        Resource = aws_cloudwatch_log_group.transactions_firehose.arn
      },
      {
        Sid    = "CWLogsDeliveryWrite"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
        ]
        Resource = "${aws_cloudwatch_log_group.transactions_firehose.arn}:*"
      }
    ]
  })
}

resource "aws_kinesis_stream" "transactions" {
  name             = "${var.project_name}-transactions"
  shard_count      = 1
  retention_period = 24

  tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}

resource "aws_kinesis_firehose_delivery_stream" "transactions_to_raw_s3" {
  name        = "${var.project_name}-transactions-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.transactions.arn
    role_arn           = aws_iam_role.transactions_firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.transactions_firehose.arn
    bucket_arn = aws_s3_bucket.datalake.arn

    prefix              = "raw/transactions/"
    error_output_prefix = "raw/transactions-errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffering_size      = 5
    buffering_interval  = 60
    compression_format  = "UNCOMPRESSED"
    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.transactions_firehose.name
      log_stream_name = aws_cloudwatch_log_stream.transactions_firehose_s3.name
    }
  }

  tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}
