# =======================================================
# Terraform Outputs
# =======================================================

output "s3_bucket_name" {
  description = "Prod S3 bucket name"
  value       = aws_s3_bucket.prod_bucket.id
}

output "s3_bucket_arn" {
  description = "Prod S3 bucket ARN"
  value       = aws_s3_bucket.prod_bucket.arn
}

output "databricks_role_arn" {
  description = "Databricks IAM role ARN — use in Unity Catalog storage credential"
  value       = aws_iam_role.databricks_s3_role.arn
}

output "glue_role_arn" {
  description = "Glue IAM role ARN"
  value       = aws_iam_role.glue_role.arn
}

output "glue_job_name" {
  description = "Glue job name"
  value       = aws_glue_job.dynamodb_export.name
}

output "aws_account_id" {
  description = "AWS Account ID"
  value       = data.aws_caller_identity.current.account_id
}