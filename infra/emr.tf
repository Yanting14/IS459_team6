# EMR Serverless: the Spark application, the role its jobs run as, and the job code.
# Training and Analyse reuse the same application.

locals {
  prepare_entry_point = "s3://${aws_s3_bucket.curated.id}/${aws_s3_object.prepare_job.key}"
  emr_log_uri         = "s3://${aws_s3_bucket.curated.id}/logs/"
}

resource "aws_emrserverless_application" "spark" {
  name = "${var.project}-spark"
  # Keep this release's Spark version in step with pyspark in prepare/requirements-dev.txt.
  release_label = "emr-7.8.0"
  type          = "spark"

  # Jobs start cold rather than holding warm workers; the pipeline runs rarely.
  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  maximum_capacity {
    cpu    = "32 vCPU"
    memory = "128 GB"
  }
}

# --- Job code -----------------------------------------------------------------

resource "aws_s3_object" "prepare_job" {
  bucket = aws_s3_bucket.curated.id
  key    = "code/prepare.py"
  source = "${path.module}/../prepare/prepare.py"
  etag   = filemd5("${path.module}/../prepare/prepare.py")
}

# --- Job role -----------------------------------------------------------------

data "aws_iam_policy_document" "emr_serverless_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_job" {
  name               = "${var.project}-emr-job"
  assume_role_policy = data.aws_iam_policy_document.emr_serverless_assume.json
}

resource "aws_iam_role_policy" "emr_job" {
  name = "read-raw-write-curated"
  role = aws_iam_role.emr_job.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = [aws_s3_bucket.raw.arn, aws_s3_bucket.curated.arn]
      },
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.raw.arn}/${var.raw_prefix}/*"
      },
      {
        # DeleteObject is what Spark's overwrite mode uses to clear a partition. No "/"
        # before the "*": EMRFS writes directory markers beside the prefix, not under it.
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.curated.arn}/${var.curated_prefix}*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.curated.arn}/logs/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.curated.arn}/${aws_s3_object.prepare_job.key}"
      },
    ]
  })
}
