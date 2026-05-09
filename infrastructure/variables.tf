# Input variables for naming defaults and bootstrap settings for the fraud detection Terraform stack.

variable "project_name" {
  description = "Human-readable project name used for tagging and resource naming prefixes"
  type        = string
  default     = "fraud-detection"
}

variable "aws_region" {
  description = "AWS region for the provider (the S3 backend is configured separately below with a literal region)."
  type        = string
  default     = "us-east-1"
}

variable "your_name" {
  description = "Owner or engineer identifier embedded in tags or conventions (personalize locally when applying)"
  type        = string
  default     = "yourname"
}

variable "alert_email" {
  description = "Email address for pipeline alerts"
  type        = string
}
