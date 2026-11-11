# Tear down the project

The brief requires every AWS resource to be deleted after the course. `scripts/teardown.sh` deletes
everything Terraform made, then the hand-made state bucket, then lists anything still tagged
`Project=is459-team6`. This runbook is the order to do it in and the steps the script cannot do.

## When

After the Week 13 presentation, and after the final cost report for the slides
(`python scripts/cost_report.py`). Cost Explorer keeps working once the resources are gone, but the
budget and its alert emails do not: the budget (`infra/budget.tf`) is destroyed with everything else.

Make sure no pipeline run or EMR Serverless job is running, and that the `is459-team6-spark`
application shows `STOPPED` in EMR Studio (or `aws emr-serverless list-applications`). It stops by
itself 15 minutes after its last job; the destroy fails on an application that is still `STARTED`.

## Run it

From the repository root, in Git Bash or Linux, with AWS credentials for the team account and
`infra/` initialised (`terraform -chdir=infra init`):

```bash
scripts/teardown.sh --dry-run   # prints each change instead of making it; lookups still run
scripts/teardown.sh
```

A dry run against the live account exits non-zero: it deletes nothing, so the sweep at the end
still finds every resource. That is expected.

It prints the account and region (both read from `infra/providers.tf`), and changes nothing unless
you type the account ID back exactly, with no spaces. Then:

1. **Targeted apply with `force_destroy_buckets=true`.** The flag only takes effect once it is in
   state, so it is applied first to the resources that read it: the raw, curated, models and gold
   buckets (the gold bucket also holds `code/dashboard.zip`), the Athena workgroup and the scoring
   ECR repository.
2. **`terraform destroy -var force_destroy_buckets=true`.** Terraform shows the plan and asks for
   `yes`. This removes everything else, including the budget, the dashboard instance (if
   `enable_dashboard` was on), its role and security group, and both secrets,
   `is459-team6/kaggle` and `is459-team6/dashboard-password`. Neither secret has a recovery
   window, so both are gone at once.
3. **The state bucket.** Its name is read from `infra/providers.tf`. The script first checks that
   `terraform -chdir=infra state list` prints nothing; if the destroy left anything in state, it
   stops non-zero and keeps the bucket. Otherwise every object version and delete marker is
   deleted, then the bucket. It also stops if any version fails to delete.
4. **The tag sweep.** The Resource Groups Tagging API lists anything still tagged
   `Project=is459-team6`. The script exits non-zero if anything is left.

If the script stops part-way, fix the cause and run it again: it picks up where it left off. When
the state is already empty it skips steps 1 and 2 (so it never re-creates anything with the
targeted apply), and when the state bucket is already gone it skips straight to the sweep.

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
  tagged, so the sweep cannot see them. Check IAM -> Users. Service-linked roles that AWS made
  (such as `AWSServiceRoleForAmazonEMRServerless`) cost nothing and can stay.
- **Remove leftover log groups.** Some services (Lambda, EMR Serverless, Step Functions, Batch)
  create their own CloudWatch log groups, and those carry no project tag, so the sweep cannot see
  them. Look in CloudWatch -> Log groups yourself for groups under `/aws/...` named after the
  project, `/ecs/is459-team6-*` and `/batch/...`, and delete any that are left.
- **Check Billing a day later.** Cost data lags about a day, and with the budget gone, nothing
  sends an alert. Billing -> Bills should show no new charges after the teardown day.
