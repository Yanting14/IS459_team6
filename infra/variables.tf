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

variable "bq1_date" {
  description = "Default dataset day BQ1 scores; a historical replay, not the wall-clock date."
  type        = string
  default     = "2022-09-15"
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
