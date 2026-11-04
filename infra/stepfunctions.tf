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

      # Placeholder until the EMR Serverless prepare job exists.
      Prepare = {
        Type = "Pass"
        End  = true
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
        Resource = [aws_iam_role.ingest_execution.arn, aws_iam_role.ingest_task.arn]
      },
      {
        # runTask.sync waits for the task via this managed EventBridge rule.
        Effect   = "Allow"
        Action   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
        Resource = "arn:aws:events:${var.region}:${local.account_id}:rule/StepFunctionsGetEventsForECSTaskRule"
      },
    ]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name       = "${var.project}-pipeline"
  role_arn   = aws_iam_role.pipeline.arn
  definition = jsonencode(local.pipeline_definition)
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
