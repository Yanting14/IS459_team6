# Tear down the project

The brief requires every AWS resource to be deleted after the course. `scripts/teardown.sh` deletes
everything Terraform made, then the hand-made state bucket, then lists anything still tagged
`Project=is459-team6`. This runbook is the order to do it in and the steps the script cannot do.

## When

After the Week 13 presentation, and after the final cost report for the slides
(`python scripts/cost_report.py`). Cost Explorer keeps working once the resources are gone, but the
budget and its alert emails do not: the budget (`infra/budget.tf`) is destroyed with everything else.

## Run it

From the repository root, in Git Bash or Linux, with AWS credentials for the team account and
`infra/` initialised (`terraform -chdir=infra init`):

```bash
scripts/teardown.sh --dry-run   # prints each change instead of making it; lookups still run
scripts/teardown.sh
```

It prints the account and region, and changes nothing unless you type the account ID back exactly.
Then:

1. **Targeted apply with `force_destroy_buckets=true`.** The flag only takes effect once it is in
   state, so it is applied first to the resources that read it: the raw, curated, models and gold
   buckets (the gold bucket also holds `code/dashboard.zip`), the Athena workgroup and the scoring
   ECR repository.
2. **`terraform destroy -var force_destroy_buckets=true`.** Terraform shows the plan and asks for
   `yes`. This removes everything else, including the budget, the dashboard instance (if
   `enable_dashboard` was on), its role and security group, and both secrets,
   `is459-team6/kaggle` and `is459-team6/dashboard-password`. Neither secret has a recovery
   window, so both are gone at once.
3. **The state bucket.** Its name is read from `infra/providers.tf`. Every object version and delete
   marker is deleted, then the bucket. From here on there is no Terraform state, so a failed step 2
   must be fixed and re-run before this one.
4. **The tag sweep.** The Resource Groups Tagging API lists anything still tagged
   `Project=is459-team6`. The script exits non-zero if anything is left.

If the sweep lists something, run it again a few minutes later, because the API can take a while to
drop a deleted resource. To run only the sweep:

```bash
aws resourcegroupstaggingapi get-resources --tag-filters Key=Project,Values=is459-team6 \
  --query 'ResourceTagMappingList[].ResourceARN' --output text
```

Anything still listed after that, delete by hand in its console.

## What the script cannot do

- **Revoke the Kaggle token** at kaggle.com -> Settings -> API Tokens. Deleting the secret does not
  revoke it.
- **Delete IAM users or access keys made by hand.** Terraform never made them, and they are not
  tagged. Check IAM -> Users.
- **Remove leftover log groups.** Some services create their own CloudWatch log groups, and those
  carry no project tag. Remove any the sweep lists, and look in CloudWatch -> Log groups for others
  named after the project.
- **Check Billing a day later.** Cost data lags about a day, and with the budget gone, nothing
  sends an alert. Billing -> Bills should show no new charges after the teardown day.
