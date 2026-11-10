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
}
mock_provider "archive" {}

variables {
  curated_prefix      = "ibm-aml/test"
  athena_queue_run    = "run-1"
  athena_create_views = false # the views run a local script against real Athena
}

run "tables_follow_the_pipeline_prefixes" {
  command = apply
  assert {
    condition     = aws_glue_catalog_table.transfers.storage_descriptor[0].location == "s3://${aws_s3_bucket.curated.id}/ibm-aml/test/transfers/"
    error_message = "transfers must read the curated prefix"
  }
  assert {
    condition     = aws_glue_catalog_table.queue_files.parameters["storage.location.template"] == "s3://${aws_s3_bucket.scoring["gold"].id}/bq1/queue/run-1/date=$${date}/"
    error_message = "queue_files must read the configured queue run"
  }
  assert {
    condition     = aws_glue_catalog_table.sharing_files.storage_descriptor[0].location == "s3://${aws_s3_bucket.scoring["gold"].id}/ibm-aml/test/sharing/"
    error_message = "sharing_files must read the gold sharing folder"
  }
}

run "partitioned_tables_project_the_date" {
  command = apply
  assert {
    condition     = alltrue([for t in [aws_glue_catalog_table.transfers, aws_glue_catalog_table.transfer_labels, aws_glue_catalog_table.queue_files] : t.parameters["projection.enabled"] == "true"])
    error_message = "date partitions must be projected so nothing registers them"
  }
}

run "workgroup_limits_scans" {
  command = apply
  assert {
    condition     = aws_athena_workgroup.this.configuration[0].bytes_scanned_cutoff_per_query == 10737418240
    error_message = "the workgroup must stop queries over 10 GiB"
  }
  assert {
    condition     = aws_athena_workgroup.this.configuration[0].enforce_workgroup_configuration
    error_message = "clients must not override the results location or limit"
  }
}
