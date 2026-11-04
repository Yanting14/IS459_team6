terraform {
  # use_lockfile needs 1.10 or newer.
  required_version = ">= 1.10"

  # The team shares one state, so it lives in S3, with S3's own lockfile keeping two
  # applies apart. This bucket cannot be managed by this configuration, because it has
  # to exist before the backend loads; the README has the one-off commands that made it.
  backend "s3" {
    bucket       = "is459-team6-tfstate-207567757101"
    key          = "infra/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.region

  # Refuse to run against any account other than the team's.
  allowed_account_ids = ["207567757101"]

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
}
