# Entirely mocked plans/applies: no credentials, state backend or AWS API calls.
mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "207567757101" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  mock_data "aws_ec2_instance_type_offerings" {
    defaults = { locations = ["us-east-1a"] }
  }
  mock_data "aws_subnets" {
    defaults = { ids = ["subnet-mock"] }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::207567757101:role/mock-role" }
  }
  mock_resource "aws_iam_instance_profile" {
    defaults = { arn = "arn:aws:iam::207567757101:instance-profile/mock-profile" }
  }
  mock_resource "aws_iam_policy" {
    defaults = { arn = "arn:aws:iam::207567757101:policy/mock-policy" }
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
  mock_resource "aws_athena_workgroup" {
    defaults = { arn = "arn:aws:athena:us-east-1:207567757101:workgroup/mock" }
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

run "disabled_creates_only_the_password_secret" {
  command = plan
  assert {
    condition     = length(aws_instance.dashboard) == 0 && length(aws_security_group.dashboard) == 0 && length(aws_iam_role.dashboard) == 0 && length(aws_s3_object.dashboard_code) == 0
    error_message = "with enable_dashboard false there must be no instance, security group, role or code object"
  }
  assert {
    condition     = length(aws_iam_instance_profile.dashboard) == 0 && length(aws_iam_role_policy.dashboard) == 0 && length(aws_iam_role_policy_attachment.dashboard) == 0
    error_message = "with enable_dashboard false there must be no instance profile, inline policy or policy attachment"
  }
  assert {
    condition     = aws_secretsmanager_secret.dashboard_password.name == "is459-team6/dashboard-password" && aws_secretsmanager_secret.dashboard_password.recovery_window_in_days == 0
    error_message = "the password secret must exist with no recovery window"
  }
  assert {
    condition     = output.dashboard_url == null
    error_message = "dashboard_url must be null when disabled"
  }
}

run "enabled_instance_is_locked_down" {
  command = apply
  variables {
    enable_dashboard = true
  }
  # Two default subnets in AZs that offer the instance type: the choice must not depend on API order.
  override_data {
    target = data.aws_subnets.dashboard
    values = { ids = ["subnet-b", "subnet-a"] }
  }
  assert {
    condition     = aws_instance.dashboard[0].subnet_id == "subnet-a"
    error_message = "the instance must go in the first (sorted) default subnet of an AZ offering its type"
  }
  assert {
    condition     = strcontains(aws_instance.dashboard[0].user_data, "User=dashboard")
    error_message = "the dashboard service must run as the unprivileged dashboard user, not root"
  }
  assert {
    condition     = aws_instance.dashboard[0].metadata_options[0].http_tokens == "required" && aws_instance.dashboard[0].metadata_options[0].http_put_response_hop_limit == 1
    error_message = "IMDSv2 must be required with a hop limit of 1"
  }
  assert {
    condition     = aws_instance.dashboard[0].credit_specification[0].cpu_credits == "standard"
    error_message = "the instance must use standard CPU credits"
  }
  assert {
    condition     = alltrue([for r in aws_security_group.dashboard[0].ingress : !(r.from_port <= 22 && r.to_port >= 22)])
    error_message = "nothing may be open on port 22"
  }
  assert {
    condition     = [for r in aws_security_group.dashboard[0].ingress : "${r.protocol}:${r.from_port}-${r.to_port}"] == ["tcp:8501-8501"]
    error_message = "the only ingress must be TCP 8501"
  }
  assert {
    condition     = [for r in aws_security_group.dashboard[0].egress : "${r.protocol}:${r.from_port}-${r.to_port}"] == ["tcp:443-443"]
    error_message = "the only egress must be TCP 443"
  }
  assert {
    condition     = !strcontains(aws_iam_role_policy.dashboard[0].policy, "s3:PutObject") && !strcontains(aws_iam_role_policy.dashboard[0].policy, "s3:DeleteObject")
    error_message = "the instance's inline policy must not write or delete S3 objects"
  }
  assert {
    condition     = strcontains(aws_iam_role_policy.dashboard[0].policy, "/bq1/*/metrics.json")
    error_message = "the instance must read the BQ1 models' metrics.json (Review queue page)"
  }
  assert {
    condition     = startswith(output.dashboard_url, "http://") && endswith(output.dashboard_url, ":8501")
    error_message = "dashboard_url must be http://<public DNS>:8501"
  }
}
