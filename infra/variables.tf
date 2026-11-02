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
