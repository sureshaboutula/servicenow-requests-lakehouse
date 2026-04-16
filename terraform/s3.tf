# =======================================================
# S3 Bucket — Prod Data Lake
# =======================================================

# Main prod bucket
resource "aws_s3_bucket" "prod_bucket" {
  bucket = "dynamodb-project-exports-${var.env}"
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "prod_bucket" {
  bucket = aws_s3_bucket.prod_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket owner enforced (required by Databricks)
resource "aws_s3_bucket_ownership_controls" "prod_bucket" {
  bucket = aws_s3_bucket.prod_bucket.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Enable versioning
resource "aws_s3_bucket_versioning" "prod_bucket" {
  bucket = aws_s3_bucket.prod_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

# -------------------------------------------------------
# Folder Structure
# -------------------------------------------------------
resource "aws_s3_object" "processed_requests" {
  bucket  = aws_s3_bucket.prod_bucket.id
  key     = "processed_requests/"
  content = ""
}

resource "aws_s3_object" "bronze_folder" {
  bucket  = aws_s3_bucket.prod_bucket.id
  key     = "db-bronze/${var.env}/"
  content = ""
}

resource "aws_s3_object" "silver_folder" {
  bucket  = aws_s3_bucket.prod_bucket.id
  key     = "db-silver/${var.env}/"
  content = ""
}

resource "aws_s3_object" "gold_folder" {
  bucket  = aws_s3_bucket.prod_bucket.id
  key     = "db-gold/${var.env}/"
  content = ""
}

resource "aws_s3_object" "checkpoint_folder" {
  bucket  = aws_s3_bucket.prod_bucket.id
  key     = "checkpoints/${var.env}/"
  content = ""
}

resource "aws_s3_object" "glue_scripts_folder" {
  bucket  = aws_s3_bucket.prod_bucket.id
  key     = "glue-scripts/"
  content = ""
}

resource "aws_s3_object" "metadata_folder" {
  bucket  = aws_s3_bucket.prod_bucket.id
  key     = "metadata/"
  content = ""
}

# -------------------------------------------------------
# Bucket Policy — Allow Databricks Access
# -------------------------------------------------------
resource "aws_s3_bucket_policy" "prod_bucket" {
  bucket = aws_s3_bucket.prod_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root"
        }
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.prod_bucket.arn,
          "${aws_s3_bucket.prod_bucket.arn}/*"
        ]
      }
    ]
  })
}