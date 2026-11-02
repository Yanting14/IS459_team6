data "archive_file" "check_raw" {
  type        = "zip"
  source_dir  = "${path.module}/../lambdas/check_raw"
  output_path = "${path.module}/build/check_raw.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "check_raw" {
  name               = "${var.project}-check-raw"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "check_raw_logs" {
  role       = aws_iam_role.check_raw.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "check_raw_s3" {
  name = "read-success-marker"
  role = aws_iam_role.check_raw.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.raw.arn
      },
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.raw.arn}/${var.raw_prefix}/_SUCCESS"
      },
    ]
  })
}

resource "aws_cloudwatch_log_group" "check_raw" {
  name              = "/aws/lambda/${var.project}-check-raw"
  retention_in_days = 30
}

resource "aws_lambda_function" "check_raw" {
  function_name    = "${var.project}-check-raw"
  role             = aws_iam_role.check_raw.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.check_raw.output_path
  source_code_hash = data.archive_file.check_raw.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      RAW_BUCKET = aws_s3_bucket.raw.id
      RAW_PREFIX = var.raw_prefix
    }
  }

  depends_on = [aws_cloudwatch_log_group.check_raw]
}
