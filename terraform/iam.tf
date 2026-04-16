# =======================================================
# IAM Roles
# =======================================================

# -------------------------------------------------------
# Databricks S3 Storage Credential Role
# -------------------------------------------------------
resource "aws_iam_role" "databricks_s3_role" {
  name = "databricks-s3-${var.env}-credential-role"

  # Step 1: Create with only Databricks trust
  # Self-assume is added AFTER role exists (below)
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksAssume"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_account_id
          }
        }
      }
    ]
  })
}

# Step 2: Update trust policy AFTER role exists
# Uses ignore_changes to avoid conflicts on re-apply
resource "aws_iam_role_policies_exclusive" "databricks_trust" {
  role_name = aws_iam_role.databricks_s3_role.name

  policy_names = []
}

# Patch trust policy to add self-assume after role is created
resource "null_resource" "databricks_self_assume" {
  depends_on = [aws_iam_role.databricks_s3_role]

  provisioner "local-exec" {
    command = "aws iam update-assume-role-policy --role-name databricks-s3-${var.env}-credential-role --policy-document file://databricks_trust_policy.json"
  }
}

# S3 access policy for Databricks
resource "aws_iam_role_policy" "databricks_s3_policy" {
  name = "databricks-s3-${var.env}-policy"
  role = aws_iam_role.databricks_s3_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
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

# -------------------------------------------------------
# Glue IAM Role
# -------------------------------------------------------
resource "aws_iam_role" "glue_role" {
  name = "glue-${var.env}-export-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "glue-${var.env}-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.prod_bucket.arn,
          "${aws_s3_bucket.prod_bucket.arn}/*"
        ]
      },
      {
        Sid    = "DynamoDBAccess"
        Effect = "Allow"
        Action = [
          "dynamodb:Scan",
          "dynamodb:GetItem",
          "dynamodb:DescribeTable"
        ]
        Resource = [
          data.aws_dynamodb_table.service_requests.arn
        ]
      },
      {
        Sid      = "GlueAccess"
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = ["*"]
      }
    ]
  })
}