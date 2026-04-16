# =======================================================
# Terraform Main Configuration
# servicenow-requests-lakehouse — AWS Infrastructure
# =======================================================

terraform {
  required_version = ">= 1.0"

  # Remote state — stored in S3
  backend "s3" {
    bucket  = "servicenow-lakehouse-tfstate-176713589038"
    key     = "prod/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

# AWS Provider
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.env
      ManagedBy   = "Terraform"
    }
  }
}

provider "null" {}

# Get current AWS account info
data "aws_caller_identity" "current" {}

# Get existing DynamoDB table
data "aws_dynamodb_table" "service_requests" {
  name = var.dynamodb_table_name
}