# Account-wide, and ANNUALLY because the budget is one total for a project that runs within 2026;
# a monthly budget would reset and never see it.
resource "aws_budgets_budget" "project" {
  name              = "${var.project}-total"
  budget_type       = "COST"
  limit_amount      = tostring(var.budget_usd)
  limit_unit        = "USD"
  time_unit         = "ANNUALLY"
  time_period_start = "2026-01-01_00:00"

  dynamic "notification" {
    for_each = [["ACTUAL", 50], ["ACTUAL", 80], ["ACTUAL", 100], ["FORECASTED", 100]]
    content {
      notification_type          = notification.value[0]
      threshold                  = notification.value[1]
      threshold_type             = "PERCENTAGE"
      comparison_operator        = "GREATER_THAN"
      subscriber_email_addresses = var.alert_emails
    }
  }
}
