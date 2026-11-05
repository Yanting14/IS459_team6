locals {
  pipeline_definition = {
    Comment = "IS459 AML pipeline"
    StartAt = "CheckRawZone"
    States = {
      CheckRawZone = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.check_raw.arn
          Payload      = {}
        }
        ResultSelector = { "ingested.$" = "$.Payload.ingested" }
        ResultPath     = "$.raw"
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
          IntervalSeconds = 2
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        Next = "AlreadyIngested"
      }

      AlreadyIngested = {
        Type = "Choice"
        Choices = [{
          Variable      = "$.raw.ingested"
          BooleanEquals = true
          Next          = "Prepare"
        }]
        Default = "Ingest"
      }

      # The script skips files already in S3, so a retry resumes where it failed.
      Ingest = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync"
        Parameters = {
          Cluster        = aws_ecs_cluster.main.arn
          TaskDefinition = aws_ecs_task_definition.ingest.arn
          LaunchType     = "FARGATE"
          NetworkConfiguration = {
            AwsvpcConfiguration = {
              Subnets        = data.aws_subnets.default_public.ids
              SecurityGroups = [aws_security_group.ingest.id]
              AssignPublicIp = "ENABLED"
            }
          }
        }
        ResultPath     = null
        TimeoutSeconds = 14400
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 60
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Next = "Prepare"
      }

      Prepare = {
        Type     = "Task"
        Resource = "arn:aws:states:::emr-serverless:startJobRun.sync"
        Parameters = {
          ApplicationId           = aws_emrserverless_application.spark.id
          ExecutionRoleArn        = aws_iam_role.emr_job.arn
          Name                    = "${var.project}-prepare"
          ExecutionTimeoutMinutes = 60
          JobDriver = {
            SparkSubmit = {
              EntryPoint = local.prepare_entry_point
              EntryPointArguments = [
                "--input", "s3://${aws_s3_bucket.raw.id}/${var.raw_prefix}",
                "--output", "s3://${aws_s3_bucket.curated.id}",
                "--version-prefix", var.curated_prefix,
              ]
            }
          }
          ConfigurationOverrides = {
            MonitoringConfiguration = {
              S3MonitoringConfiguration = { LogUri = local.emr_log_uri }
            }
          }
        }
        # A backstop above the job's own 60-minute limit, in case .sync never hears back.
        TimeoutSeconds = 4200
        # Only service-level transients are retried: a job that failed on its own data has
        # already spent its hour, and the quality gate would reach the same verdict again.
        Retry = [{
          ErrorEquals     = ["EmrServerless.ThrottlingException", "EmrServerless.InternalServerException"]
          IntervalSeconds = 30
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        # Keep execution-supplied scoring options; EMR's response is not input.
        ResultPath = null
        Next       = "ScoringConfiguration"
      }

      PipelineComplete = { Type = "Succeed" }

      ScoringConfiguration = {
        Type       = "Pass"
        Result     = var.enable_scoring
        ResultPath = "$.scoring_enabled"
        Next       = "ScoringEnabled"
      }

      ScoringEnabled = {
        Type    = "Choice"
        Choices = [{ Variable = "$.scoring_enabled", BooleanEquals = true, Next = "ScoringDefaults" }]
        Default = "PipelineComplete"
      }

      ScoringDefaults = {
        Type = "Pass"
        Result = {
          start = var.scoring_start, cutoff = var.scoring_cutoff, k = var.scoring_k, bank = ""
        }
        ResultPath = "$.scoring_defaults"
        Next       = "HasScoringOptions"
      }

      HasScoringOptions = {
        Type    = "Choice"
        Choices = [{ Variable = "$.scoring", IsPresent = true, Next = "MergeScoringOptions" }]
        Default = "DefaultScoringOptions"
      }

      DefaultScoringOptions = {
        Type       = "Pass"
        Parameters = { "options.$" = "$.scoring_defaults" }
        ResultPath = "$.scoring_request"
        Next       = "Score"
      }

      MergeScoringOptions = {
        Type       = "Pass"
        Parameters = { "options.$" = "States.JsonMerge($.scoring_defaults, $.scoring, false)" }
        ResultPath = "$.scoring_request"
        Next       = "Score"
      }

      Score = {
        Type     = "Task"
        Resource = "arn:aws:states:::batch:submitJob.sync"
        Parameters = {
          JobName       = "${var.project}-scoring"
          JobQueue      = aws_batch_job_queue.scoring.arn
          JobDefinition = aws_batch_job_definition.scoring.arn
          ContainerOverrides = {
            Environment = [
              { Name = "SCORING_START", "Value.$" = "$.scoring_request.options.start" },
              { Name = "SCORING_CUTOFF", "Value.$" = "$.scoring_request.options.cutoff" },
              { Name = "SCORING_K", "Value.$" = "States.Format('{}', $.scoring_request.options.k)" },
              { Name = "SCORING_BANK", "Value.$" = "$.scoring_request.options.bank" },
              { Name = "EXECUTION_ID", "Value.$" = "$$.Execution.Name" },
            ]
          }
        }
        # Covers two host-failure attempts plus queue/provisioning time. The .sync
        # integration attempts to terminate the Batch job if this task is aborted.
        TimeoutSeconds = 2 * var.scoring_timeout_seconds + 3600
        ResultSelector = { "job_id.$" = "$.JobId" }
        ResultPath     = "$.scoring_job"
        # Batch owns host retries. Do not retry model/data failures in Step Functions.
        Catch = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.scoring_error", Next = "ScoringFailed" }]
        Next  = "ScoringResult"
      }

      ScoringFailed = {
        Type  = "Fail"
        Error = "ScoringFailed"
        Cause = "Batch scoring failed or timed out. Inspect scoring_error and the Batch CloudWatch logs."
      }

      ScoringResult = {
        Type = "Pass"
        Parameters = {
          "job_id.$"     = "$.scoring_job.job_id"
          "result_uri.$" = "States.Format('s3://${aws_s3_bucket.scoring["gold"].id}/${local.scoring_gold_prefix}/executions/{}.json', $$.Execution.Name)"
        }
        ResultPath = "$.scoring_result"
        End        = true
      }
    }
  }
}

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${var.project}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

resource "aws_iam_role_policy" "pipeline" {
  name = "run-pipeline-steps"
  role = aws_iam_role.pipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "batch:SubmitJob"
        Resource = [aws_batch_job_queue.scoring.arn, aws_batch_job_definition.scoring.arn]
      },
      {
        # Job IDs are allocated at runtime; DescribeJobs has no resource scoping.
        Effect   = "Allow"
        Action   = ["batch:DescribeJobs", "batch:TerminateJob"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = aws_lambda_function.check_raw.arn
      },
      {
        Effect   = "Allow"
        Action   = "ecs:RunTask"
        Resource = "arn:aws:ecs:${var.region}:${local.account_id}:task-definition/${aws_ecs_task_definition.ingest.family}:*"
      },
      {
        Effect   = "Allow"
        Action   = ["ecs:StopTask", "ecs:DescribeTasks"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = [aws_iam_role.ingest_execution.arn, aws_iam_role.ingest_task.arn, aws_iam_role.emr_job.arn]
      },
      {
        Effect   = "Allow"
        Action   = "emr-serverless:StartJobRun"
        Resource = aws_emrserverless_application.spark.arn
      },
      {
        # startJobRun.sync polls the run and tags it to track ownership.
        Effect   = "Allow"
        Action   = ["emr-serverless:GetJobRun", "emr-serverless:CancelJobRun", "emr-serverless:TagResource"]
        Resource = [aws_emrserverless_application.spark.arn, "${aws_emrserverless_application.spark.arn}/jobruns/*"]
      },
      {
        # Every .sync step waits on an EventBridge rule that Step Functions creates and
        # names itself, one per integration. UpdateStateMachine checks this role can create
        # each of them, so a missing name fails the apply rather than the first run.
        Effect = "Allow"
        Action = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
        Resource = [
          "arn:aws:events:${var.region}:${local.account_id}:rule/StepFunctionsGetEventsForECSTaskRule",
          "arn:aws:events:${var.region}:${local.account_id}:rule/StepFunctionsGetEventsForEMRServerlessJobRule",
          "arn:aws:events:${var.region}:${local.account_id}:rule/StepFunctionsGetEventsForBatchJobsRule",
        ]
      },
    ]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name       = "${var.project}-pipeline"
  role_arn   = aws_iam_role.pipeline.arn
  definition = jsonencode(local.pipeline_definition)

  lifecycle {
    precondition {
      condition     = !var.enable_scoring || var.scoring_image_digest != ""
      error_message = "Publish the scoring image and set its immutable digest before enabling scoring."
    }
  }

  # The definition names the role's permissions but not the policy holding them, and the
  # API validates them as it writes. Without this, a new .sync step can be applied before
  # the permission it needs.
  depends_on = [aws_iam_role_policy.pipeline]
}

# --- Daily trigger ------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "daily" {
  name                = "${var.project}-daily"
  description         = "Start the pipeline daily at 02:00 SGT"
  schedule_expression = "cron(0 18 * * ? *)"
  state               = var.enable_daily_schedule ? "ENABLED" : "DISABLED"
}

data "aws_iam_policy_document" "events_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events" {
  name               = "${var.project}-events"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
}

resource "aws_iam_role_policy" "events" {
  name = "start-pipeline"
  role = aws_iam_role.events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.pipeline.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "daily" {
  rule     = aws_cloudwatch_event_rule.daily.name
  arn      = aws_sfn_state_machine.pipeline.arn
  role_arn = aws_iam_role.events.arn
}
