output "region" {
  value = var.region
}

output "raw_bucket" {
  value = aws_s3_bucket.raw.id
}

output "kaggle_secret_name" {
  value = aws_secretsmanager_secret.kaggle.name
}

output "ingest_ecr_repository_url" {
  value = aws_ecr_repository.ingest.repository_url
}

output "pipeline_state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "kaggle_files" {
  value = local.kaggle_files
}
