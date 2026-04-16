# =======================================================
# Glue Job — DynamoDB Export
# =======================================================

# Upload Glue script to S3
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.prod_bucket.id
  key    = "glue-scripts/dynamodb_export.py"
  source = "../glue/dynamodb_export.py"
  etag   = filemd5("../glue/dynamodb_export.py")
}

# Create Glue Job
resource "aws_glue_job" "dynamodb_export" {
  name     = "${var.project_name}-${var.env}-export"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.prod_bucket.id}/glue-scripts/dynamodb_export.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--project_bucket"                   = aws_s3_bucket.prod_bucket.id
    "--metadata_key"                     = "metadata/watermark_${var.env}.json"
    "--table_name"                       = var.dynamodb_table_name
    "--athena_db"                        = "servicenow_${var.env}"
    "--athena_table"                     = "processed_requests"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }

  glue_version      = "4.0"
  number_of_workers = var.glue_num_workers
  worker_type       = var.glue_worker_type

  execution_property {
    max_concurrent_runs = 1
  }
}

# Glue trigger — scheduled
resource "aws_glue_trigger" "daily_export" {
  name     = "${var.project_name}-${var.env}-trigger"
  type     = "SCHEDULED"
  schedule = var.glue_schedule

  actions {
    job_name = aws_glue_job.dynamodb_export.name
  }
}