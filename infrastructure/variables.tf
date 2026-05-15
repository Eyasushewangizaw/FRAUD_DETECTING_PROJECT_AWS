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

variable "redshift_master_username" {
  description = "Database superuser name for the Redshift cluster (cannot be reserved names such as 'admin' on some platforms—use fraud_admin style identifiers)."
  type        = string
  default     = "fraud_admin"
}

variable "redshift_allowed_cidr_blocks" {
  description = "IPv4 CIDR blocks allowed to reach Redshift on port 5439 (include your public IP /32 for psql and Airflow)."
  type        = list(string)
}

variable "redshift_node_type" {
  description = "Redshift provisioned node type for the single-node cluster (dc2.large is the smallest commonly available class)."
  type        = string
  default     = "dc2.large"
}

variable "redshift_apply_bootstrap_sql" {
  description = "When true, run idempotent CREATE SCHEMA/TABLE statements through the Redshift Data API after the cluster is available (requires redshift-data:ExecuteStatement on the principal running Terraform)."
  type        = bool
  default     = true
}
