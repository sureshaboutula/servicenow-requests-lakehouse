# =======================================================
# Terraform Variables
# =======================================================

variable "env" {
  description = "Deployment environment"
  type        = string
  default     = "prod"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "servicenow-requests-lakehouse"
}

variable "dynamodb_table_name" {
  description = "DynamoDB source table name"
  type        = string
  default     = "service_request_tracking"
}

variable "databricks_account_id" {
  description = "Databricks account UUID for IAM trust policy"
  type        = string
  default     = "6555e35c-1b70-48f0-abd0-ef2bb37bfcb1"
}

variable "glue_worker_type" {
  description = "Glue worker type"
  type        = string
  default     = "G.1X"
}

variable "glue_num_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 2
}

variable "glue_schedule" {
  description = "Glue job schedule (cron)"
  type        = string
  default     = "cron(0 6 * * ? *)"  # 6 AM daily UTC
}