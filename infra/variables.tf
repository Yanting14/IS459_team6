variable "region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  description = "Prefix for resource names."
  type        = string
  default     = "is459-team6"
}

variable "kaggle_dataset" {
  type    = string
  default = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
}

variable "dataset_variants" {
  description = "HI = higher illicit ratio, LI = lower illicit ratio."
  type        = list(string)
  default     = ["HI"]
}

variable "dataset_sizes" {
  type    = list(string)
  default = ["Medium", "Large"]
}

variable "raw_prefix" {
  description = "Key prefix in the raw bucket. Bump the version to force a fresh ingest."
  type        = string
  default     = "ibm-aml/v1"
}

variable "curated_prefix" {
  description = "Key prefix in the curated bucket. Bump the version to force a fresh build."
  type        = string
  default     = "ibm-aml/v2"
}

variable "ingest_image_tag" {
  type    = string
  default = "latest"
}

variable "enable_daily_schedule" {
  description = "Turn on the daily EventBridge trigger once the rest of the pipeline exists."
  type        = bool
  default     = false
}

variable "force_destroy_buckets" {
  description = "Allow terraform destroy to delete buckets that still contain data."
  type        = bool
  default     = false
}

variable "enable_scoring" {
  description = "Route Prepare to Batch scoring after the image and model have been published."
  type        = bool
  default     = false
}

variable "scoring_image_digest" {
  description = "Immutable ECR digest returned by the scoring image publish script."
  type        = string
  default     = ""
  validation {
    condition     = var.scoring_image_digest == "" || can(regex("^sha256:[0-9a-f]{64}$", var.scoring_image_digest))
    error_message = "Use a sha256 digest, not a mutable image tag."
  }
}

variable "scoring_model_version" {
  type    = string
  default = "6e5e2da7ed914e14"
}

variable "scoring_model_prefix" {
  type    = string
  default = "gin/6e5e2da7ed914e14"
}

variable "scoring_start" {
  description = "Default dataset day; this is a historical replay, not the wall-clock date."
  type        = string
  default     = "2022-09-14"
}

variable "scoring_cutoff" {
  description = "Exclusive end of the default one-day scoring window."
  type        = string
  default     = "2022-09-15"
}

variable "scoring_k" {
  description = "Daily review budget; historical top-1000 metrics do not validate this daily policy."
  type        = number
  default     = 1000
  validation {
    condition     = var.scoring_k >= 1 && var.scoring_k <= 1000000 && floor(var.scoring_k) == var.scoring_k
    error_message = "scoring_k must be a positive integer no larger than 1000000."
  }
}

variable "scoring_disk_gib" {
  type    = number
  default = 100
  validation {
    condition     = var.scoring_disk_gib >= 100 && floor(var.scoring_disk_gib) == var.scoring_disk_gib
    error_message = "Graph staging requires at least 100 GiB of scratch storage."
  }
}

variable "scoring_timeout_seconds" {
  description = "Per Batch attempt; Step Functions also limits queueing and retries."
  type        = number
  default     = 7200
  validation {
    condition     = var.scoring_timeout_seconds >= 60 && floor(var.scoring_timeout_seconds) == var.scoring_timeout_seconds
    error_message = "Batch timeout must be an integer of at least 60 seconds."
  }
}

variable "bq1_model_version" {
  description = "Pinned BQ1 model under s3://<models>/bq1/. Empty skips BQ1 scoring unless an execution retrains."
  type        = string
  default     = ""
}

variable "bq1_dates" {
  description = "Default dataset days BQ1 scores, one queue each; a historical replay, not the wall-clock date."
  type        = list(string)
  default     = ["2022-09-15", "2022-09-16"]
  validation {
    condition     = length(var.bq1_dates) > 0 && alltrue([for d in var.bq1_dates : can(regex("^\\d{4}-\\d{2}-\\d{2}$", d))])
    error_message = "bq1_dates must list at least one YYYY-MM-DD day."
  }
}

variable "bq1_k" {
  description = "BQ1 daily review budget."
  type        = number
  default     = 1000
  validation {
    condition     = var.bq1_k >= 1 && floor(var.bq1_k) == var.bq1_k
    error_message = "bq1_k must be a positive integer."
  }
}

variable "athena_queue_run" {
  description = "BQ1 queue run (the Step Functions execution name) under s3://<gold>/bq1/queue/ that the queue_files table reads."
  type        = string
  default     = "bq1-bankvis-20261109"
}

variable "athena_date_range" {
  description = "Days the date partition is projected over, start and end."
  type        = string
  default     = "2022-09-01,2022-09-30"
}

variable "athena_scan_limit_gib" {
  description = "Per-query scan limit of the Athena workgroup; the whole transfers table is about 1.4 GB."
  type        = number
  default     = 10
}

variable "athena_create_views" {
  description = "Create the two Athena views by running 05_views.sql from the machine that applies (needs python3, boto3, AWS credentials)."
  type        = bool
  default     = true
}

variable "enable_bq2" {
  description = "Run the BQ2 bank-vs-shared comparison after Score. Needs a scoring image that contains bq2/ (rebuilt after this was added)."
  type        = bool
  default     = false
}

variable "bq2_dates" {
  description = "Dataset days BQ2 compares; a historical replay, not the wall-clock date."
  type        = list(string)
  default     = ["2022-09-15", "2022-09-16"]
  validation {
    condition     = length(var.bq2_dates) > 0 && alltrue([for d in var.bq2_dates : can(regex("^\\d{4}-\\d{2}-\\d{2}$", d))])
    error_message = "bq2_dates must list at least one YYYY-MM-DD day."
  }
}

variable "bq2_banks" {
  description = "Banks compared in BQ2: the 16 in bq2/runs/banks_final.csv (at least 10 positive accounts on each of 15 and 16 Sep)."
  type        = list(string)
  default     = ["000", "012", "020", "011", "02310", "00214", "01213", "003", "025", "02597", "001", "006", "01208", "01831", "027", "029"]
  validation {
    condition     = length(var.bq2_banks) > 0 && alltrue([for b in var.bq2_banks : can(regex("^[0-9A-Za-z]+$", b))])
    error_message = "bq2_banks must list at least one bank id (letters and digits)."
  }
}

variable "bq2_ratio" {
  description = "BQ2 review budget as a share of each bank's eligible accounts (bq2.sharing compare --ratio)."
  type        = number
  default     = 0.0011
}

variable "bq2_floor" {
  description = "Smallest BQ2 review budget per bank and day (bq2.sharing compare --floor)."
  type        = number
  default     = 10
}

variable "alert_emails" {
  description = "Who the budget alerts email. The repo is public: set this in an uncommitted infra/*.tfvars or TF_VAR_alert_emails, never in team.auto.tfvars.json. Empty means nobody is notified."
  type        = list(string)
  default     = []
}

variable "budget_usd" {
  description = "The project's total AWS budget for 2026, in USD."
  type        = number
  default     = 250
}
