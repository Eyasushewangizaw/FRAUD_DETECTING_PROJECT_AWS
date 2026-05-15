# CloudWatch alarms + SNS fan-out for Glue, Kinesis, and Lambda operational failures.

resource "aws_sns_topic" "fraud_pipeline_alerts" {
  name = "fraud-pipeline-alerts"

  tags = {
    Project     = var.project_name
    Environment = "dev"
  }
}

resource "aws_sns_topic_subscription" "fraud_pipeline_alerts_email" {
  topic_arn = aws_sns_topic.fraud_pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  alarm_name          = "${var.project_name}-glue-job-failure"
  alarm_description   = "Alert when Glue job failures are detected in a 5-minute window."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Failed"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = aws_glue_job.fraud_etl.name
  }

  alarm_actions = [aws_sns_topic.fraud_pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.fraud_pipeline_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  alarm_name          = "${var.project_name}-kinesis-iterator-age"
  alarm_description   = "Alert when stream consumer lag exceeds 60s."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 300
  statistic           = "Maximum"
  threshold           = 60000
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.transactions.name
  }

  alarm_actions = [aws_sns_topic.fraud_pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.fraud_pipeline_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "lambda_error" {
  alarm_name          = "${var.project_name}-lambda-errors"
  alarm_description   = "Alert when Lambda function errors exceed 5 in 5 minutes."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.glue_trigger.function_name
  }

  alarm_actions = [aws_sns_topic.fraud_pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.fraud_pipeline_alerts.arn]
}
