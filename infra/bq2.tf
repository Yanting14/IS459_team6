# BQ2 in the pipeline: after the GIN's Score step, score each day once on the shared graph and once
# per bank (BQ2Score, on the same Batch queue and job definition as Score), then compare the two
# views (BQ2Compare) and publish sharing/comparison.csv, which the Athena table sharing_files reads.
# Replaces bq2/runs/submit_batch.sh + fetch_runs.py + `sharing compare` run by hand.
#
# The compare job reads labels, so it runs under its own role; the scoring role still cannot
# (ADR-0004). It uses the scoring image, which must be rebuilt to contain bq2/ before enable_bq2.

locals {
  bq2_executions_uri = "s3://${aws_s3_bucket.scoring["gold"].id}/${local.scoring_gold_prefix}/executions"
  bq2_sharing_prefix = "${var.curated_prefix}/sharing"

  # One item per (day, view): the shared run first, then each bank. The execution ids are the ones
  # bq2/runs used by hand, so receipts from those runs and from the pipeline are interchangeable.
  # Scoring reuses a completed run with the same inputs (scoring/cloud.py), so a rerun is cheap.
  bq2_items = flatten([
    for d in var.bq2_dates : [
      for b in concat([""], var.bq2_banks) : {
        start        = d
        cutoff       = formatdate("YYYY-MM-DD", timeadd("${d}T00:00:00Z", "24h"))
        bank         = b
        execution_id = b == "" ? "bq2-shared-${d}" : "bq2-bank-${b}-${d}"
      }
    ]
  ])

  bq2_states = {
    BQ2Configuration = {
      Type       = "Pass"
      Result     = var.enable_bq2
      ResultPath = "$.bq2_enabled"
      Next       = "BQ2Enabled"
    }

    BQ2Enabled = {
      Type    = "Choice"
      Choices = [{ Variable = "$.bq2_enabled", BooleanEquals = true, Next = "BQ2Runs" }]
      Default = "PipelineComplete"
    }

    BQ2Runs = {
      Type       = "Pass"
      Result     = local.bq2_items
      ResultPath = "$.bq2_runs"
      Next       = "BQ2Score"
    }

    # The compute environment runs one 4-vCPU job at a time, so more concurrency only queues in Batch.
    BQ2Score = {
      Type           = "Map"
      ItemsPath      = "$.bq2_runs"
      MaxConcurrency = 2
      ItemProcessor = {
        ProcessorConfig = { Mode = "INLINE" }
        StartAt         = "BQ2ScoreView"
        States = {
          BQ2ScoreView = {
            Type     = "Task"
            Resource = "arn:aws:states:::batch:submitJob.sync"
            Parameters = {
              "JobName.$"   = "States.Format('${var.project}-scoring-{}', $.execution_id)"
              JobQueue      = aws_batch_job_queue.scoring.arn
              JobDefinition = aws_batch_job_definition.scoring.arn
              ContainerOverrides = {
                Environment = [
                  { Name = "SCORING_START", "Value.$" = "$.start" },
                  { Name = "SCORING_CUTOFF", "Value.$" = "$.cutoff" },
                  { Name = "SCORING_K", Value = tostring(var.scoring_k) },
                  { Name = "SCORING_BANK", "Value.$" = "$.bank" },
                  { Name = "EXECUTION_ID", "Value.$" = "$.execution_id" },
                ]
              }
            }
            TimeoutSeconds = 2 * var.scoring_timeout_seconds + 3600
            ResultPath     = null
            End            = true
          }
        }
      }
      ResultPath = null
      Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.bq2_error", Next = "BQ2Failed" }]
      Next       = "BQ2Compare"
    }

    BQ2Compare = {
      Type     = "Task"
      Resource = "arn:aws:states:::batch:submitJob.sync"
      Parameters = {
        JobName       = "${var.project}-bq2-compare"
        JobQueue      = aws_batch_job_queue.scoring.arn
        JobDefinition = aws_batch_job_definition.bq2_compare.arn
      }
      TimeoutSeconds = 3600 + 1800
      ResultPath     = null
      Catch          = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.bq2_error", Next = "BQ2Failed" }]
      Next           = "PipelineComplete"
    }

    BQ2Failed = {
      Type  = "Fail"
      Error = "BQ2Failed"
      Cause = "A BQ2 scoring run or the compare job failed. Inspect bq2_error and the Batch CloudWatch logs."
    }
  }
}

resource "aws_iam_role" "bq2_compare" {
  name               = "${var.project}-bq2-compare"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy" "bq2_compare" {
  name = "read-runs-and-labels-publish-sharing"
  role = aws_iam_role.bq2_compare.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow", Action = "s3:ListBucket"
        Resource = aws_s3_bucket.curated.arn
        Condition = { StringLike = { "s3:prefix" = [
          "${var.curated_prefix}/transfers/*", "${var.curated_prefix}/transfer_labels/*",
        ] } }
      },
      {
        Effect = "Allow", Action = "s3:GetObject"
        Resource = [
          "${aws_s3_bucket.curated.arn}/${var.curated_prefix}/transfers/*",
          "${aws_s3_bucket.curated.arn}/${var.curated_prefix}/transfer_labels/*",
          "${aws_s3_bucket.scoring["gold"].arn}/${local.scoring_gold_prefix}/executions/*",
          "${aws_s3_bucket.scoring["gold"].arn}/${local.scoring_gold_prefix}/predictions/*",
        ]
      },
      {
        Effect   = "Allow", Action = ["s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.scoring["gold"].arn}/${local.bq2_sharing_prefix}/*"
      },
    ]
  })
}

resource "aws_batch_job_definition" "bq2_compare" {
  name                  = "${var.project}-bq2-compare"
  type                  = "container"
  platform_capabilities = ["EC2"]
  container_properties = jsonencode({
    image            = local.scoring_image
    command          = ["python", "-m", "bq2.cloud"]
    jobRoleArn       = aws_iam_role.bq2_compare.arn
    executionRoleArn = aws_iam_role.scoring_execution.arn
    resourceRequirements = [
      { type = "VCPU", value = "4" },
      { type = "MEMORY", value = "12288" },
    ]
    environment = [
      { name = "AWS_DEFAULT_REGION", value = var.region },
      { name = "CURATED_URI", value = "s3://${aws_s3_bucket.curated.id}/${var.curated_prefix}" },
      { name = "EXECUTIONS_URI", value = local.bq2_executions_uri },
      { name = "SHARING_URI", value = "s3://${aws_s3_bucket.scoring["gold"].id}/${local.bq2_sharing_prefix}" },
      { name = "BQ2_DATES", value = join(",", var.bq2_dates) },
      { name = "BQ2_BANKS", value = join(",", var.bq2_banks) },
      { name = "BQ2_RATIO", value = tostring(var.bq2_ratio) },
      { name = "BQ2_FLOOR", value = tostring(var.bq2_floor) },
      { name = "BQ2_MODEL", value = "gin" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.scoring.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "bq2-compare"
      }
    }
  })
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
    attempt_duration_seconds = 3600
  }
  depends_on = [aws_iam_role_policy.bq2_compare, aws_iam_role_policy_attachment.scoring_execution]
}
