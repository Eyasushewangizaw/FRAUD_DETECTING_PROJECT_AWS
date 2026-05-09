# IAM principals for Lambda triggers, Glue ETL jobs, Redshift COPY/Spectrum ingestion, and MWAA orchestration.
# Roles use resource-level ARNs for this stack’s datalake buckets; APIs that AWS does not scope to ARNs remain
# documented inline for review when tightening policies further.

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  iam_tags = {
    Project     = var.project_name
    Environment = "dev"
  }

  # Glue naming: database segment must satisfy Glue identifier rules (hyphens -> underscores).
  glue_database_name = replace(var.project_name, "-", "_")

  glue_catalog_arn    = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog"
  glue_database_arn   = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${local.glue_database_name}"
  glue_all_tables_arn = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${local.glue_database_name}/*"
}

# -----------------------------------------------------------------------------
# Glue ETL service role — assumed by AWS Glue job runs to read/write lake objects,
# mutate the shared Data Catalog for this project database, and emit job logs.
# Referenced by Lambda (PassRole) when starting jobs and by MWAA operators.
# -----------------------------------------------------------------------------
resource "aws_iam_role" "glue_etl" {
  name = "${var.project_name}-glue-etl-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.iam_tags
}

resource "aws_iam_role_policy" "glue_etl" {
  name = "${var.project_name}-glue-etl"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatalakeS3ReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
          "s3:ListBucketMultipartUploads",
        ]
        Resource = "${aws_s3_bucket.datalake.arn}/*"
      },
      {
        Sid    = "DatalakeS3List"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = aws_s3_bucket.datalake.arn
      },
      {
        Sid    = "GlueDataCatalogThisDatabase"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions",
          "glue:GetPartition",
          "glue:BatchGetPartition",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchDeleteTable",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition",
        ]
        Resource = [
          local.glue_catalog_arn,
          local.glue_database_arn,
          local.glue_all_tables_arn,
        ]
      },
      {
        Sid    = "GlueJobSelfManagement"
        Effect = "Allow"
        Action = [
          "glue:GetJob",
          "glue:GetJobs",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:GetJobBookmarks",
          "glue:UpdateJobBookmark",
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${var.project_name}-*",
        ]
      },
      {
        Sid    = "GlueCloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:DescribeLogStreams",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
      },
    ]
  })
}

# -----------------------------------------------------------------------------
# Lambda pipeline trigger — invokes Glue asynchronously; emits function logs only.
# AWSLambdaBasicExecutionRole grants CreateLogStream/PutLogEvents on /aws/lambda/<fn>.
# iam:PassRole is limited to the Glue execution role above and only when calling Glue APIs.
# -----------------------------------------------------------------------------
resource "aws_iam_role" "lambda_trigger" {
  name = "${var.project_name}-lambda-trigger-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.iam_tags
}

resource "aws_iam_role_policy_attachment" "lambda_trigger_basic_logging" {
  role       = aws_iam_role.lambda_trigger.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_trigger_invoke_glue" {
  name = "${var.project_name}-lambda-trigger-glue"
  role = aws_iam_role.lambda_trigger.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "StartAndInspectGlueJobs"
        Effect = "Allow"
        Action = [
          "glue:GetJob",
          "glue:GetJobs",
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${var.project_name}-*",
        ]
      },
      {
        Sid    = "PassGlueServiceRoleToJobRun"
        Effect = "Allow"
        Action = [
          "iam:PassRole",
        ]
        Resource = aws_iam_role.glue_etl.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "glue.amazonaws.com"
          }
        }
      },
    ]
  })
}

# -----------------------------------------------------------------------------
# Redshift COPY role — attached to a Redshift cluster/workgroup for loading narrow
# prefixes (curated + optional raw read) from the shared datalake bucket only.
# -----------------------------------------------------------------------------
resource "aws_iam_role" "redshift_lake_read" {
  name = "${var.project_name}-redshift-lake-read-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.iam_tags
}

resource "aws_iam_role_policy" "redshift_lake_read" {
  name = "${var.project_name}-redshift-lake-read"
  role = aws_iam_role.redshift_lake_read.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListLakePrefixes"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
        ]
        Resource = aws_s3_bucket.datalake.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "raw/*",
              "curated/*",
            ]
          }
        }
      },
      {
        Sid    = "ReadLakeObjectsForCopy"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
        ]
        Resource = [
          "${aws_s3_bucket.datalake.arn}/raw/*",
          "${aws_s3_bucket.datalake.arn}/curated/*",
        ]
      },
    ]
  })
}

# -----------------------------------------------------------------------------
# MWAA execution role — Airflow tasks call AWS APIs to orchestrate Glue, Redshift,
# and object storage; also read DAG artifacts from the MWAA bucket and emit metrics/logs.
# Trust is limited to the managed Airflow/MWAA services in this account.
# PassRole allows submitting Glue jobs that require the dedicated Glue service role.
# -----------------------------------------------------------------------------
resource "aws_iam_role" "mwaa_execution" {
  name = "${var.project_name}-mwaa-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "airflow.amazonaws.com",
            "airflow-env.amazonaws.com",
          ]
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = local.iam_tags
}

resource "aws_iam_role_policy" "mwaa_execution" {
  name = "${var.project_name}-mwaa-execution"
  role = aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "MwaaBucketFullAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketVersions",
        ]
        Resource = [
          aws_s3_bucket.mwaa.arn,
          "${aws_s3_bucket.mwaa.arn}/*",
        ]
      },
      {
        Sid    = "DatalakeReadForSensors"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetObject",
        ]
        Resource = [
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*",
        ]
      },
      {
        Sid    = "GlueJobManagement"
        Effect = "Allow"
        Action = [
          "glue:GetJob",
          "glue:GetJobs",
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:GetCrawler",
          "glue:GetCrawlers",
          "glue:StartCrawler",
          "glue:StopCrawler",
          "glue:ListCrawlers",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          local.glue_catalog_arn,
          local.glue_database_arn,
          local.glue_all_tables_arn,
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${var.project_name}-*",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:crawler/${var.project_name}-*",
        ]
      },
      {
        Sid    = "PassGlueServiceRole"
        Effect = "Allow"
        Action = [
          "iam:PassRole",
        ]
        Resource = aws_iam_role.glue_etl.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "glue.amazonaws.com"
          }
        }
      },
      {
        Sid    = "RedshiftDescribeAndDataAPI"
        Effect = "Allow"
        Action = [
          "redshift:DescribeClusters",
          "redshift:DescribeClusterSubnetGroups",
          "redshift-data:ExecuteStatement",
          "redshift-data:DescribeStatement",
          "redshift-data:GetStatementResult",
          "redshift-data:CancelStatement",
          "redshift-data:ListStatements",
        ]
        # Redshift Data API does not support fine-grained ARNs; restrict by statements issued from approved DAGs.
        Resource = "*"
      },
      {
        Sid    = "CloudWatchObservation"
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "cloudwatch:GetMetricData",
          "cloudwatch:ListMetrics",
          "cloudwatch:DescribeAlarms",
        ]
        Resource = "*"
      },
      {
        Sid    = "LogsDiscovery"
        Effect = "Allow"
        Action = [
          "logs:DescribeLogGroups",
        ]
        Resource = "*"
      },
      {
        Sid    = "MwaaAirflowLogDelivery"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
          "logs:FilterLogEvents",
          "logs:GetLogEvents",
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/airflow/*:*"
      },
    ]
  })
}
