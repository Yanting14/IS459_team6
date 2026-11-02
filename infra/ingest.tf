locals {
  kaggle_files = flatten([
    for v in var.dataset_variants : [
      for s in var.dataset_sizes : [
        "${v}-${s}_Trans.csv",
        "${v}-${s}_Patterns.txt",
        "${v}-${s}_accounts.csv",
      ]
    ]
  ])
}

# --- Kaggle credentials -------------------------------------------------------
# Terraform creates the secret but never its value, so the token stays out of
# state. Fill it once with: aws secretsmanager put-secret-value (see README).

resource "aws_secretsmanager_secret" "kaggle" {
  name                    = "${var.project}/kaggle"
  description             = "Kaggle API token (KGAT_...) as a plain string"
  recovery_window_in_days = 0
}

# --- Container image ----------------------------------------------------------

resource "aws_ecr_repository" "ingest" {
  name                 = "${var.project}-ingest"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "ingest" {
  repository = aws_ecr_repository.ingest.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

# --- Networking ---------------------------------------------------------------
# Default VPC public subnets + a public IP give internet access without a NAT
# Gateway. The security group allows no inbound traffic.

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default_public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "aws_security_group" "ingest" {
  name        = "${var.project}-ingest"
  description = "Ingest task: outbound HTTPS only"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "HTTPS to Kaggle, S3, ECR, Secrets Manager"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- IAM ----------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Used by ECS itself to pull the image, write logs and inject the secret.
resource "aws_iam_role" "ingest_execution" {
  name               = "${var.project}-ingest-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "ingest_execution_managed" {
  role       = aws_iam_role.ingest_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "ingest_execution_secret" {
  name = "read-kaggle-secret"
  role = aws_iam_role.ingest_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = aws_secretsmanager_secret.kaggle.arn
    }]
  })
}

# Used by the script inside the container.
resource "aws_iam_role" "ingest_task" {
  name               = "${var.project}-ingest-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy" "ingest_task_s3" {
  name = "write-raw-zone"
  role = aws_iam_role.ingest_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # ListBucket makes HeadObject return 404 rather than 403 for missing keys.
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.raw.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.raw.arn}/${var.raw_prefix}/*"
      },
    ]
  })
}

# --- ECS ----------------------------------------------------------------------

resource "aws_ecs_cluster" "main" {
  name = var.project
}

resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/ecs/${var.project}-ingest"
  retention_in_days = 30
}

resource "aws_ecs_task_definition" "ingest" {
  family                   = "${var.project}-ingest"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 2048
  memory                   = 4096
  execution_role_arn       = aws_iam_role.ingest_execution.arn
  task_role_arn            = aws_iam_role.ingest_task.arn

  # Peak usage is one zip plus one unzipped CSV (~16 GB for the Large files).
  ephemeral_storage {
    size_in_gib = 60
  }

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "ingest"
    image     = "${aws_ecr_repository.ingest.repository_url}:${var.ingest_image_tag}"
    essential = true
    environment = [
      { name = "RAW_BUCKET", value = aws_s3_bucket.raw.id },
      { name = "RAW_PREFIX", value = var.raw_prefix },
      { name = "KAGGLE_DATASET", value = var.kaggle_dataset },
      { name = "KAGGLE_FILES", value = join(",", local.kaggle_files) },
    ]
    secrets = [
      { name = "KAGGLE_API_TOKEN", valueFrom = aws_secretsmanager_secret.kaggle.arn },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.ingest.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ingest"
      }
    }
  }])
}
