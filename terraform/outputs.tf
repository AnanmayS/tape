output "aws_region" {
  description = "Region the stack was applied to. Every AWS CLI call in docs/deploy.md needs it."
  value       = var.aws_region
}

output "bucket_name" {
  description = "Capture bucket. This is the -s3-bucket argument for `tape verify` and `tape replay`."
  value       = aws_s3_bucket.captures.id
}

output "bucket_arn" {
  description = "Capture bucket ARN."
  value       = aws_s3_bucket.captures.arn
}

output "ecr_repository_url" {
  description = "Push the ingester image here; the task definition runs <this>:<image_tag>."
  value       = aws_ecr_repository.ingester.repository_url
}

output "cluster_name" {
  description = "ECS cluster running the one ingester task."
  value       = aws_ecs_cluster.main.name
}

output "service_name" {
  description = "ECS service. Force a redeploy with: aws ecs update-service --cluster <cluster> --service <this> --force-new-deployment"
  value       = aws_ecs_service.ingester.name
}

output "log_group" {
  description = "CloudWatch Logs group. Tail with: aws logs tail <this> --follow"
  value       = aws_cloudwatch_log_group.ingester.name
}

output "alarm_topic_arn" {
  description = "SNS topic the gap and ingest-lag alarms publish to."
  value       = aws_sns_topic.alarms.arn
}

output "metrics_namespace" {
  description = "CloudWatch namespace the ingester publishes to, and the only one its task role may write."
  value       = var.metrics_namespace
}

output "capture_key_prefix" {
  description = "The single key prefix the task role may PutObject under."
  value       = local.key_prefix
}
