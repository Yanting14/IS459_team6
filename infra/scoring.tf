# A single Batch worker keeps graph arrays on encrypted local scratch storage.
# Compute scales to zero; enable_scoring controls the Step Functions route.
locals {
  scoring_image       = var.scoring_image_digest == "" ? "${aws_ecr_repository.scoring.repository_url}:bootstrap" : "${aws_ecr_repository.scoring.repository_url}@${var.scoring_image_digest}"
  scoring_gold_prefix = "${var.curated_prefix}/scoring"
}

resource "aws_ecr_repository" "scoring" {
  name                 = "${var.project}-scoring"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = var.force_destroy_buckets
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_cloudwatch_log_group" "scoring" {
  name              = "/batch/${var.project}-scoring"
  retention_in_days = 30
}

resource "aws_iam_role" "scoring_instance" {
  name = "${var.project}-scoring-instance"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ec2.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "scoring_instance" {
  role       = aws_iam_role.scoring_instance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "scoring" {
  name = "${var.project}-scoring"
  role = aws_iam_role.scoring_instance.name
}

resource "aws_iam_role" "scoring_execution" {
  name               = "${var.project}-scoring-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "scoring_execution" {
  role       = aws_iam_role.scoring_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "scoring_job" {
  name               = "${var.project}-scoring-job"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy" "scoring_job" {
  name = "stage-model-and-transfers-publish-gold"
  role = aws_iam_role.scoring_job.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow", Action = "s3:ListBucket"
        Resource = [aws_s3_bucket.curated.arn, aws_s3_bucket.scoring["models"].arn, aws_s3_bucket.scoring["gold"].arn]
      },
      {
        Effect = "Allow", Action = "s3:GetObject"
        Resource = [
          "${aws_s3_bucket.curated.arn}/${var.curated_prefix}/_SUCCESS",
          "${aws_s3_bucket.curated.arn}/${var.curated_prefix}/transfers/*",
          "${aws_s3_bucket.scoring["models"].arn}/${var.scoring_model_prefix}/*",
          "${aws_s3_bucket.scoring["gold"].arn}/${local.scoring_gold_prefix}/*",
        ]
      },
      {
        Effect   = "Allow", Action = ["s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.scoring["gold"].arn}/${local.scoring_gold_prefix}/*"
      },
    ]
  })
}

resource "aws_launch_template" "scoring" {
  name_prefix = "${var.project}-scoring-"
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.scoring_disk_gib
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
}

resource "aws_batch_compute_environment" "scoring" {
  name_prefix = "${var.project}-scoring-"
  type        = "MANAGED"
  state       = "ENABLED"
  # Omitting service_role uses AWSServiceRoleForBatch. Bootstrap credentials need
  # permission to create that service-linked role if the account has none yet.
  compute_resources {
    type                = "EC2"
    allocation_strategy = "BEST_FIT_PROGRESSIVE"
    min_vcpus           = 0
    max_vcpus           = 4
    instance_type       = ["m7i.xlarge"]
    instance_role       = aws_iam_instance_profile.scoring.arn
    subnets             = data.aws_subnets.default_public.ids
    security_group_ids  = [aws_security_group.ingest.id]
    ec2_configuration {
      image_type = "ECS_AL2023"
    }
    launch_template {
      launch_template_id = aws_launch_template.scoring.id
      version            = aws_launch_template.scoring.latest_version
    }
  }
  lifecycle {
    create_before_destroy = true
  }
  depends_on = [aws_iam_role_policy_attachment.scoring_instance]
}

resource "aws_batch_job_queue" "scoring" {
  name     = "${var.project}-scoring"
  state    = "ENABLED"
  priority = 1
  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.scoring.arn
  }
}

resource "aws_batch_job_definition" "scoring" {
  name                  = "${var.project}-scoring"
  type                  = "container"
  platform_capabilities = ["EC2"]
  container_properties = jsonencode({
    image            = local.scoring_image
    jobRoleArn       = aws_iam_role.scoring_job.arn
    executionRoleArn = aws_iam_role.scoring_execution.arn
    resourceRequirements = [
      { type = "VCPU", value = "4" },
      { type = "MEMORY", value = "12288" },
    ]
    environment = [
      { name = "AWS_DEFAULT_REGION", value = var.region },
      { name = "CURATED_URI", value = "s3://${aws_s3_bucket.curated.id}/${var.curated_prefix}" },
      { name = "MODEL_URI", value = "s3://${aws_s3_bucket.scoring["models"].id}/${var.scoring_model_prefix}" },
      { name = "MODEL_VERSION", value = var.scoring_model_version },
      { name = "GOLD_URI", value = "s3://${aws_s3_bucket.scoring["gold"].id}/${local.scoring_gold_prefix}" },
      { name = "IMAGE_REVISION", value = local.scoring_image },
      { name = "SCORING_DEVICE", value = "cpu" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.scoring.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "scoring"
      }
    }
  })
  # Retry lost EC2 hosts, but fail data/model errors without repeating hours of work.
  retry_strategy {
    attempts = 2
    evaluate_on_exit {
      action           = "RETRY"
      on_status_reason = "Host EC2*"
    }
    evaluate_on_exit {
      action    = "EXIT"
      on_reason = "*"
    }
  }
  timeout {
    attempt_duration_seconds = var.scoring_timeout_seconds
  }
  depends_on = [aws_iam_role_policy.scoring_job, aws_iam_role_policy_attachment.scoring_execution]
}

# Separate buckets preserve ADR-0003: models are versioned; derived gold is not.
resource "aws_s3_bucket" "scoring" {
  for_each      = toset(["models", "gold"])
  bucket        = "${var.project}-${each.key}-${local.account_id}"
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_versioning" "models" {
  bucket = aws_s3_bucket.scoring["models"].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scoring" {
  for_each = aws_s3_bucket.scoring
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "scoring" {
  for_each                = aws_s3_bucket.scoring
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "scoring_tls_only" {
  for_each = aws_s3_bucket.scoring
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Deny", Principal = "*", Action = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.scoring]
}

resource "aws_s3_bucket_lifecycle_configuration" "scoring" {
  for_each = aws_s3_bucket.scoring
  bucket   = each.value.id
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}
