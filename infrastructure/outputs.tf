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
