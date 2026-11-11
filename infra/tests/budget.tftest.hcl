# Entirely mocked plans: no credentials, state backend or AWS API calls.
mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "207567757101" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
}
mock_provider "archive" {}

variables {
  athena_create_views = false # the views run a local script against real Athena
}

run "budget_covers_the_project_total" {
  command = plan
  variables {
    alert_emails = ["alerts@example.com"]
  }
  assert {
    condition     = aws_budgets_budget.project.budget_type == "COST" && tonumber(aws_budgets_budget.project.limit_amount) == 250 && aws_budgets_budget.project.limit_unit == "USD"
    error_message = "the budget must be US$250 of cost"
  }
  assert {
    condition     = aws_budgets_budget.project.time_unit == "ANNUALLY" && startswith(aws_budgets_budget.project.time_period_start, "2026-01-01")
    error_message = "the budget must total the year from 2026-01-01, not reset monthly"
  }
  assert {
    condition = toset([for n in aws_budgets_budget.project.notification : "${n.notification_type}:${n.threshold}:${n.threshold_type}:${n.comparison_operator}"]) == toset([
      "ACTUAL:50:PERCENTAGE:GREATER_THAN",
      "ACTUAL:80:PERCENTAGE:GREATER_THAN",
      "ACTUAL:100:PERCENTAGE:GREATER_THAN",
      "FORECASTED:100:PERCENTAGE:GREATER_THAN",
    ])
    error_message = "alerts must fire at 50/80/100% actual and 100% forecast"
  }
  assert {
    condition     = alltrue([for n in aws_budgets_budget.project.notification : n.subscriber_email_addresses == toset(["alerts@example.com"])])
    error_message = "every alert must email alert_emails"
  }
}

run "budget_plans_without_recipients" {
  command = plan
  assert {
    condition     = length(aws_budgets_budget.project.notification) == 4
    error_message = "the budget must still plan with no alert_emails"
  }
}
