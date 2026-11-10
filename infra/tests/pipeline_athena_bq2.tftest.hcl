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
  mock_resource "aws_iam_policy" {
    defaults = { arn = "arn:aws:iam::207567757101:policy/mock-policy" }
  }
  mock_resource "aws_athena_workgroup" {
    defaults = { arn = "arn:aws:athena:us-east-1:207567757101:workgroup/mock" }
  }
  mock_resource "aws_glue_catalog_database" {
    defaults = { arn = "arn:aws:glue:us-east-1:207567757101:database/mock" }
  }
}
mock_provider "archive" {}

variables {
  curated_prefix       = "ibm-aml/test"
  athena_create_views  = false # the views run a local script against real Athena
  enable_scoring       = true
  scoring_image_digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
  bq1_model_version    = "m1"
  bq2_dates            = ["2022-09-15", "2022-09-30"]
  bq2_banks            = ["000", "012"]
}

run "queue_table_follows_each_bq1_run" {
  command = apply
  assert {
    condition     = local.pipeline_definition.States.ScoreBQ1.Next == "PointQueueTable"
    error_message = "the queue must be re-pointed after BQ1 scores"
  }
  assert {
    condition     = local.pipeline_definition.States.PointQueueTemplate.Next == "ScoringConfiguration"
    error_message = "the GIN's scoring must still follow"
  }
  assert {
    condition     = local.pipeline_definition.States.PointQueueTable.Parameters["QueryString.$"] == "States.Format('ALTER TABLE queue_files SET LOCATION \\'s3://${aws_s3_bucket.scoring["gold"].id}/bq1/queue/{}/\\'', $$.Execution.Name)"
    error_message = "LOCATION must name this execution's queue folder, with ' escaped for States.Format"
  }
  assert {
    condition     = strcontains(local.pipeline_definition.States.PointQueueTemplate.Parameters["QueryString.$"], "/bq1/queue/{}/date=$\\{date\\}/\\'")
    error_message = "the partition template must keep a literal $${date}, escaped for States.Format"
  }
}

run "bq2_runs_each_view_then_compares" {
  command = apply
  variables {
    enable_bq2 = true
  }
  assert {
    condition     = local.pipeline_definition.States.ScoringResult.Next == "BQ2Configuration"
    error_message = "BQ2 must follow the GIN's Score step"
  }
  assert {
    condition     = length(local.bq2_items) == 6
    error_message = "two days times (shared + two banks)"
  }
  assert {
    condition     = local.bq2_items[0].execution_id == "bq2-shared-2022-09-15" && local.bq2_items[0].bank == ""
    error_message = "each day starts with its shared run"
  }
  assert {
    condition     = local.bq2_items[4].execution_id == "bq2-bank-000-2022-09-30" && local.bq2_items[4].cutoff == "2022-10-01"
    error_message = "the cutoff is the next day, across a month end"
  }
  assert {
    condition     = local.pipeline_definition.States.BQ2Score.Next == "BQ2Compare" && local.pipeline_definition.States.BQ2Compare.Next == "PipelineComplete"
    error_message = "compare runs after every view is scored"
  }
  assert {
    condition     = jsondecode(aws_batch_job_definition.bq2_compare.container_properties).command == ["python", "-m", "bq2.cloud"]
    error_message = "the compare job runs bq2.cloud in the scoring image"
  }
}

run "the_scoring_role_still_cannot_read_labels" {
  command = apply
  assert {
    condition     = !strcontains(aws_iam_role_policy.scoring_job.policy, "transfer_labels") && !strcontains(aws_iam_role_policy.scoring_job.policy, "/labels")
    error_message = "ADR-0004: scoring never reads labels"
  }
  assert {
    condition     = strcontains(aws_iam_role_policy.bq2_compare.policy, "transfer_labels")
    error_message = "the compare role is the one that reads labels"
  }
}

run "bq2_needs_scoring" {
  command = plan
  variables {
    enable_bq2     = true
    enable_scoring = false
  }
  expect_failures = [aws_sfn_state_machine.pipeline]
}
