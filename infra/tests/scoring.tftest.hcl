# Entirely mocked plans/applies: no credentials, state backend or AWS API calls.
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

# The repository defaults, not team.auto.tfvars.json, which terraform test also loads.
variables {
  enable_scoring       = false
  enable_bq2           = false
  scoring_image_digest = ""
  athena_create_views  = false # the views run a local script against real Athena
  enable_dashboard     = false # team.auto.tfvars.json turns it on for the demo weeks
}

run "scoring_disabled_by_default" {
  command = plan
  assert {
    condition     = local.pipeline_definition.States.ScoringConfiguration.Result == false
    error_message = "Scoring must stay disabled during bootstrap."
  }
  assert {
    condition     = local.pipeline_definition.States.Prepare.ResultPath == null
    error_message = "Prepare must preserve execution scoring options."
  }
}

run "enabled_routes_to_synchronous_batch" {
  command = apply
  variables {
    enable_scoring       = true
    scoring_image_digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
  assert {
    condition     = local.pipeline_definition.States.ScoringConfiguration.Result == true
    error_message = "Scoring enable flag was not propagated."
  }
  assert {
    condition     = local.pipeline_definition.States.Score.Resource == "arn:aws:states:::batch:submitJob.sync"
    error_message = "Step Functions must wait for Batch completion."
  }
  assert {
    condition     = local.pipeline_definition.States.Score.Catch[0].Next == "ScoringFailed"
    error_message = "Scoring failures must fail the execution."
  }
  assert {
    condition     = aws_batch_compute_environment.scoring.compute_resources[0].min_vcpus == 0
    error_message = "Idle scoring compute must scale to zero."
  }
  assert {
    condition = (
      toset(aws_batch_compute_environment.scoring.compute_resources[0].instance_type) == toset(["m7i.xlarge"]) &&
      aws_batch_compute_environment.scoring.compute_resources[0].ec2_configuration[0].image_type == "ECS_AL2023" &&
      length([for requirement in jsondecode(aws_batch_job_definition.scoring.container_properties).resourceRequirements : requirement if requirement.type == "GPU"]) == 0
    )
    error_message = "Measured daily scoring uses CPU instances without GPU reservations."
  }
  assert {
    condition     = endswith(jsondecode(aws_batch_job_definition.scoring.container_properties).image, var.scoring_image_digest)
    error_message = "Scoring must use the immutable image digest."
  }
  assert {
    condition     = !strcontains(aws_iam_role_policy.scoring_job.policy, "transfer_labels")
    error_message = "The scorer must not read transfer labels."
  }
}

run "cannot_enable_without_image" {
  command = plan
  variables {
    enable_scoring = true
  }
  expect_failures = [aws_sfn_state_machine.pipeline]
}
