# Root Terraform configuration: remote state backend, required providers, and AWS provider wiring for fraud detection workloads.
#
# Remote state lives in this S3 bucket (create it + enable versioning before terraform init). Bucket name is fixed here; Terraform cannot create the backend bucket in the same config.

terraform {
  required_version = ">= 1.5.0"

  backend "s3" {
    bucket  = "fraudeyasu"
    key     = "fraud-detection/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}
