output "region" {
  value = var.region
}

output "raw_bucket" {
  value = aws_s3_bucket.raw.id
}

output "curated_bucket" {
  value = aws_s3_bucket.curated.id
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

output "scoring_ecr_repository_url" {
  value = aws_ecr_repository.scoring.repository_url
}

output "scoring_model_uri" {
  value = "s3://${aws_s3_bucket.scoring["models"].id}/${var.scoring_model_prefix}"
}

output "scoring_gold_uri" {
  value = "s3://${aws_s3_bucket.scoring["gold"].id}/${local.scoring_gold_prefix}"
}

output "scoring_job_queue_arn" {
  value = aws_batch_job_queue.scoring.arn
}
