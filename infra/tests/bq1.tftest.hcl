# Entirely mocked applies: no credentials, state backend or AWS API calls.
mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "207567757101" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::207567757101:role/mock-role" }
  }
  mock_resource "aws_iam_instance_profile" {
    defaults = { arn = "arn:aws:iam::207567757101:instance-profile/mock-profile" }
  }
  mock_resource "aws_batch_compute_environment" {
    defaults = { arn = "arn:aws:batch:us-east-1:207567757101:compute-environment/mock" }
  }
  mock_resource "aws_batch_job_queue" {
    defaults = { arn = "arn:aws:batch:us-east-1:207567757101:job-queue/mock" }
  }
  mock_resource "aws_batch_job_definition" {
    defaults = { arn = "arn:aws:batch:us-east-1:207567757101:job-definition/mock:1" }
  }
  mock_resource "aws_ecr_repository" {
    defaults = { repository_url = "207567757101.dkr.ecr.us-east-1.amazonaws.com/mock" }
  }
  mock_resource "aws_sfn_state_machine" {
    defaults = { arn = "arn:aws:states:us-east-1:207567757101:stateMachine:mock" }
  }
}
mock_provider "archive" {}

variables {
  curated_prefix = "ibm-aml/test"
}

run "prepare_leads_to_bq1_then_the_gin" {
  command = apply
  assert {
    condition     = local.pipeline_definition.States.Prepare.Next == "BQ1Defaults"
    error_message = "Prepare must hand over to BQ1."
  }
  assert {
    condition     = local.pipeline_definition.States.ScoreBQ1.Next == "ScoringConfiguration"
    error_message = "BQ1 scoring must lead to the unchanged GIN scoring path."
  }
  assert {
    condition     = local.pipeline_definition.States.BQ1Defaults.Result.retrain == false
    error_message = "Retraining must be off unless an execution asks for it."
  }
}

run "retraining_scores_with_the_model_it_just_wrote" {
  command = apply
  assert {
    condition     = local.pipeline_definition.States.BQ1Retrain.Choices[0].Next == "TrainBQ1"
    error_message = "retrain = true must train first."
  }
  assert {
    condition     = local.pipeline_definition.States.TrainBQ1.Next == "UseTrainedBQ1Model"
    error_message = "Training must be followed by its own model, not the pinned one."
  }
  assert {
    condition     = strcontains(local.pipeline_definition.States.UseTrainedBQ1Model.Parameters["uri.$"], "$$.Execution.Name")
    error_message = "The trained model is named by the execution."
  }
  assert {
    condition     = strcontains(local.pipeline_definition.States.TrainBQ1.Parameters.JobDriver.SparkSubmit["EntryPointArguments.$"], "$$.Execution.Name")
    error_message = "Training must write under the execution's name."
  }
  assert {
    condition     = local.pipeline_definition.States.TrainBQ1.Resource == "arn:aws:states:::emr-serverless:startJobRun.sync"
    error_message = "Step Functions must wait for training to finish."
  }
}

run "no_pinned_model_skips_bq1_scoring" {
  command = apply
  assert {
    condition     = local.pipeline_definition.States.BQ1HasModel.Choices[0].Next == "ScoringConfiguration"
    error_message = "Without a model or a retrain, BQ1 must be skipped."
  }
  assert {
    condition     = strcontains(local.pipeline_definition.States.UsePinnedBQ1Model.Parameters["uri.$"], "$.bq1_request.options.model_version")
    error_message = "A pinned model comes from the execution's options or bq1_model_version."
  }
}

run "the_score_role_cannot_read_labels" {
  command = apply
  assert {
    condition     = !strcontains(aws_iam_role_policy.bq1_score.policy, "labels")
    error_message = "The score role must not reach any label table."
  }
  assert {
    condition     = strcontains(aws_iam_role_policy.bq1_train.policy, "ibm-aml/test/labels/*")
    error_message = "The train role reads labels under the curated prefix."
  }
  assert {
    condition     = local.pipeline_definition.States.ScoreBQ1.ItemProcessor.States.ScoreBQ1Day.Parameters.Name == "is459-team6-bq1-score"
    error_message = "ScoreBQ1 runs the score job."
  }
}

run "score_publishes_every_listed_day" {
  command = apply
  assert {
    condition     = local.pipeline_definition.States.BQ1Defaults.Result.dates == tolist(["2022-09-15", "2022-09-16"])
    error_message = "By default both test days are scored."
  }
  assert {
    condition     = local.pipeline_definition.States.ScoreBQ1.Type == "Map" && local.pipeline_definition.States.ScoreBQ1.ItemsPath == "$.bq1_request.options.dates"
    error_message = "ScoreBQ1 must run once per listed day."
  }
  assert {
    condition     = strcontains(local.pipeline_definition.States.ScoreBQ1.ItemProcessor.States.ScoreBQ1Day.Parameters.JobDriver.SparkSubmit["EntryPointArguments.$"], "/{}/date={}', $$.Execution.Name, $.date)")
    error_message = "Each day's queue goes to its own folder under the execution."
  }
  assert {
    condition     = local.pipeline_definition.States.ScoreBQ1.ItemProcessor.States.ScoreBQ1Day.Parameters.ExecutionRoleArn == aws_iam_role.bq1_score.arn
    error_message = "Every day is scored by the role that cannot read labels."
  }
}
