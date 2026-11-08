# BQ1: the Spark job that trains, baselines and scores on the seven-day features, on the
# same EMR Serverless application as Prepare. Two roles keep labels away from scoring
# (ADR-0005): only the train role can read curated labels.

locals {
  bq1_entry_point = "s3://${aws_s3_bucket.curated.id}/${aws_s3_object.bq1_job.key}"
  bq1_log_uri     = "s3://${aws_s3_bucket.curated.id}/logs/bq1/"
  bq1_curated_uri = "s3://${aws_s3_bucket.curated.id}/${var.curated_prefix}"
  bq1_models_uri  = "s3://${aws_s3_bucket.scoring["models"].id}/bq1"
  bq1_queue_uri   = "s3://${aws_s3_bucket.scoring["gold"].id}/bq1/queue"

  curated_features = "${aws_s3_bucket.curated.arn}/${var.curated_prefix}/features"
  curated_labels   = "${aws_s3_bucket.curated.arn}/${var.curated_prefix}/labels"
  models_bq1       = "${aws_s3_bucket.scoring["models"].arn}/bq1"
  gold_bq1         = "${aws_s3_bucket.scoring["gold"].arn}/bq1"

  # Reads and writes Spark needs under an output prefix. No "/" before the "*": EMRFS
  # writes directory markers beside a prefix, not under it.
  spark_write = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
}

resource "aws_s3_object" "bq1_job" {
  bucket = aws_s3_bucket.curated.id
  key    = "code/bq1.py"
  source = "${path.module}/../train/bq1.py"
  etag   = filemd5("${path.module}/../train/bq1.py")
}

# Run once as the train role before any training: Python, numpy and Spark ML on EMR.
resource "aws_s3_object" "bq1_smoke" {
  bucket = aws_s3_bucket.curated.id
  key    = "code/bq1_smoke.py"
  source = "${path.module}/../train/bq1_smoke.py"
  etag   = filemd5("${path.module}/../train/bq1_smoke.py")
}

resource "aws_iam_role" "bq1_train" {
  name               = "${var.project}-bq1-train"
  assume_role_policy = data.aws_iam_policy_document.emr_serverless_assume.json
}

resource "aws_iam_role_policy" "bq1_train" {
  name = "read-features-and-labels-write-models"
  role = aws_iam_role.bq1_train.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = [aws_s3_bucket.curated.arn, aws_s3_bucket.scoring["models"].arn, aws_s3_bucket.scoring["gold"].arn]
      },
      {
        Effect = "Allow"
        Action = "s3:GetObject"
        Resource = [
          "${local.curated_features}/*", "${local.curated_labels}/*",
          "${aws_s3_bucket.curated.arn}/${aws_s3_object.bq1_job.key}",
          "${aws_s3_bucket.curated.arn}/${aws_s3_object.bq1_smoke.key}",
        ]
      },
      {
        Effect   = "Allow"
        Action   = local.spark_write
        Resource = ["${local.models_bq1}*", "${local.gold_bq1}/baseline*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.curated.arn}/logs/bq1/*"
      },
    ]
  })
}

resource "aws_iam_role" "bq1_score" {
  name               = "${var.project}-bq1-score"
  assume_role_policy = data.aws_iam_policy_document.emr_serverless_assume.json
}

# No statement names labels/ or transfer_labels/: the job cannot read a label even by mistake.
resource "aws_iam_role_policy" "bq1_score" {
  name = "read-features-and-models-write-queue"
  role = aws_iam_role.bq1_score.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = [aws_s3_bucket.curated.arn, aws_s3_bucket.scoring["models"].arn, aws_s3_bucket.scoring["gold"].arn]
      },
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = ["${local.curated_features}/*", "${local.models_bq1}/*", "${aws_s3_bucket.curated.arn}/${aws_s3_object.bq1_job.key}"]
      },
      {
        Effect   = "Allow"
        Action   = local.spark_write
        Resource = "${local.gold_bq1}/queue*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.curated.arn}/logs/bq1/*"
      },
    ]
  })
}

# A policy of its own on the pipeline role, so the existing one is left as applied.
resource "aws_iam_role_policy" "pipeline_bq1" {
  name = "run-bq1-steps"
  role = aws_iam_role.pipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "iam:PassRole"
      Resource = [aws_iam_role.bq1_train.arn, aws_iam_role.bq1_score.arn]
    }]
  })
}

locals {
  bq1_monitoring = {
    MonitoringConfiguration = {
      S3MonitoringConfiguration = { LogUri = local.bq1_log_uri }
    }
  }
  bq1_retry = [{
    ErrorEquals     = ["EmrServerless.ThrottlingException", "EmrServerless.InternalServerException"]
    IntervalSeconds = 30
    MaxAttempts     = 3
    BackoffRate     = 2
  }]

  # Prepare -> BQ1 options -> (TrainBQ1) -> ScoreBQ1 -> the GIN's ScoringConfiguration.
  # Execution input may carry {"bq1": {"retrain": true, "date": ..., "model_version": ..., "k": ...}}.
  bq1_states = {
    BQ1Defaults = {
      Type = "Pass"
      Result = {
        retrain = false, date = var.bq1_date, model_version = var.bq1_model_version, k = var.bq1_k
      }
      ResultPath = "$.bq1_defaults"
      Next       = "HasBQ1Options"
    }

    HasBQ1Options = {
      Type    = "Choice"
      Choices = [{ Variable = "$.bq1", IsPresent = true, Next = "MergeBQ1Options" }]
      Default = "DefaultBQ1Options"
    }

    DefaultBQ1Options = {
      Type       = "Pass"
      Parameters = { "options.$" = "$.bq1_defaults" }
      ResultPath = "$.bq1_request"
      Next       = "BQ1Retrain"
    }

    MergeBQ1Options = {
      Type       = "Pass"
      Parameters = { "options.$" = "States.JsonMerge($.bq1_defaults, $.bq1, false)" }
      ResultPath = "$.bq1_request"
      Next       = "BQ1Retrain"
    }

    BQ1Retrain = {
      Type    = "Choice"
      Choices = [{ Variable = "$.bq1_request.options.retrain", BooleanEquals = true, Next = "TrainBQ1" }]
      Default = "BQ1HasModel"
    }

    # Without a pinned model and without retraining there is nothing to score with yet.
    BQ1HasModel = {
      Type    = "Choice"
      Choices = [{ Variable = "$.bq1_request.options.model_version", StringEquals = "", Next = "ScoringConfiguration" }]
      Default = "UsePinnedBQ1Model"
    }

    TrainBQ1 = {
      Type     = "Task"
      Resource = "arn:aws:states:::emr-serverless:startJobRun.sync"
      Parameters = {
        ApplicationId           = aws_emrserverless_application.spark.id
        ExecutionRoleArn        = aws_iam_role.bq1_train.arn
        Name                    = "${var.project}-bq1-train"
        ExecutionTimeoutMinutes = 120
        JobDriver = {
          SparkSubmit = {
            EntryPoint = local.bq1_entry_point
            # The execution name makes the model version, so a rerun never meets its own output.
            "EntryPointArguments.$" = "States.Array('train', '--curated', '${local.bq1_curated_uri}', '--out', States.Format('${local.bq1_models_uri}/{}', $$.Execution.Name), '--k', States.Format('{}', $.bq1_request.options.k))"
          }
        }
        ConfigurationOverrides = local.bq1_monitoring
      }
      TimeoutSeconds = 7800
      Retry          = local.bq1_retry
      ResultPath     = null
      Next           = "UseTrainedBQ1Model"
    }

    UseTrainedBQ1Model = {
      Type       = "Pass"
      Parameters = { "uri.$" = "States.Format('${local.bq1_models_uri}/{}', $$.Execution.Name)" }
      ResultPath = "$.bq1_model"
      Next       = "ScoreBQ1"
    }

    UsePinnedBQ1Model = {
      Type       = "Pass"
      Parameters = { "uri.$" = "States.Format('${local.bq1_models_uri}/{}', $.bq1_request.options.model_version)" }
      ResultPath = "$.bq1_model"
      Next       = "ScoreBQ1"
    }

    ScoreBQ1 = {
      Type     = "Task"
      Resource = "arn:aws:states:::emr-serverless:startJobRun.sync"
      Parameters = {
        ApplicationId           = aws_emrserverless_application.spark.id
        ExecutionRoleArn        = aws_iam_role.bq1_score.arn
        Name                    = "${var.project}-bq1-score"
        ExecutionTimeoutMinutes = 60
        JobDriver = {
          SparkSubmit = {
            EntryPoint              = local.bq1_entry_point
            "EntryPointArguments.$" = "States.Array('score', '--curated', '${local.bq1_curated_uri}', '--model', $.bq1_model.uri, '--date', $.bq1_request.options.date, '--k', States.Format('{}', $.bq1_request.options.k), '--out', States.Format('${local.bq1_queue_uri}/{}', $$.Execution.Name))"
          }
        }
        ConfigurationOverrides = local.bq1_monitoring
      }
      TimeoutSeconds = 4200
      Retry          = local.bq1_retry
      ResultPath     = null
      Next           = "ScoringConfiguration"
    }
  }
}
