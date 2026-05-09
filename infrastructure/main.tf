# Root Terraform configuration: remote state backend, required providers, and AWS provider wiring for fraud detection workloads.
#
# Provision the backend bucket fraud-detection-tf-state (and enable versioning) manually before terraform init — backend metadata cannot interpolate variables here.

terraform {
  required_version = ">= 1.5.0"

  backend "s3" {
    bucket  = "fraud-detection-tf-state"
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
