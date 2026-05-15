# Exported values (ARNs, names, endpoints) intended for operators, CI/CD, and integration with other stacks.

output "project_name" {
  description = "Project name used for resource naming"
  value       = var.project_name
}

output "sns_alert_topic_arn" {
  description = "SNS topic ARN receiving fraud pipeline alarm notifications"
  value       = aws_sns_topic.fraud_pipeline_alerts.arn
}

output "glue_job_failure_alarm_arn" {
  description = "CloudWatch alarm ARN for Glue job failures"
  value       = aws_cloudwatch_metric_alarm.glue_job_failure.arn
}

output "kinesis_iterator_age_alarm_arn" {
  description = "CloudWatch alarm ARN for Kinesis iterator age breach"
  value       = aws_cloudwatch_metric_alarm.kinesis_iterator_age.arn
}

output "lambda_error_alarm_arn" {
  description = "CloudWatch alarm ARN for Lambda error threshold breach"
  value       = aws_cloudwatch_metric_alarm.lambda_error.arn
}

output "datalake_bucket_name" {
  description = "S3 datalake bucket (raw + curated prefixes); set Airflow Variable fraud_datalake_bucket to this value"
  value       = aws_s3_bucket.datalake.bucket
}

output "glue_job_name" {
  description = "Glue ETL job name for Lambda and Airflow operators"
  value       = aws_glue_job.fraud_etl.name
}

output "glue_crawler_name" {
  description = "Glue crawler that registers curated Parquet in the Data Catalog"
  value       = aws_glue_crawler.fraud_curated.name
}

output "glue_catalog_database" {
  description = "Glue Data Catalog database containing curated tables"
  value       = aws_glue_catalog_database.fraud.name
}

output "lambda_trigger_function_name" {
  description = "Lambda function name started by S3 notifications on raw/transactions/"
  value       = aws_lambda_function.glue_trigger.function_name
}

output "redshift_cluster_identifier" {
  description = "Redshift cluster identifier for CLI/Data API calls"
  value       = aws_redshift_cluster.this.cluster_identifier
}

output "redshift_endpoint_address" {
  description = "Redshift writer endpoint DNS name"
  value       = aws_redshift_cluster.this.endpoint
}

output "redshift_port" {
  description = "Redshift JDBC / psql port"
  value       = aws_redshift_cluster.this.port
}

output "redshift_database_name" {
  description = "Default database created with the cluster"
  value       = aws_redshift_cluster.this.database_name
}

output "redshift_master_secret_arn" {
  description = "Secrets Manager secret holding the Redshift master username/password JSON for tools and the Data API"
  value       = aws_secretsmanager_secret.redshift_master.arn
}

output "redshift_copy_iam_role_arn" {
  description = "IAM role ARN to pass to Airflow Variable redshift_copy_iam_role_arn and to COPY commands"
  value       = aws_iam_role.redshift_lake_read.arn
}

output "redshift_master_password" {
  description = "Generated master password (store securely; also mirrored in Secrets Manager)"
  value       = random_password.redshift_master.result
  sensitive   = true
}
