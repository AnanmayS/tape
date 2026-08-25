# Tape — capture infrastructure.
#
# Three AWS services, each load-bearing: S3 holds the captures, ECS Fargate
# runs the one ingester, CloudWatch carries its logs, metrics and alarms. ECR
# and IAM are here because a container has to come from somewhere and has to
# run as someone, not because they are a fourth and fifth service.
#
# Nothing here is highly available, replicated or auto-scaled. There is one
# ingester on one task, and a second one would capture the same frames twice.

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

# Default VPC unless told otherwise. Creating a VPC here would mean creating a
# NAT gateway to reach the exchange from a private subnet, and that is $32 a
# month to hide a task that accepts no inbound connections at all.
data "aws_vpc" "default" {
  count = var.vpc_id == "" ? 1 : 0

  default = true
}

data "aws_subnets" "default" {
  count = length(var.subnet_ids) > 0 ? 0 : 1

  filter {
    name   = "vpc-id"
    values = [local.vpc_id]
  }
}

locals {
  name   = var.project
  vpc_id = var.vpc_id != "" ? var.vpc_id : data.aws_vpc.default[0].id

  subnet_ids = length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.default[0].ids

  bucket_name = var.bucket_name != "" ? var.bucket_name : format(
    "%s-captures-%s-%s", var.project, data.aws_caller_identity.current.account_id, var.aws_region
  )

  # Every key the capture path writes begins with the layout version, so the
  # PutObject grant can end there rather than at the bucket. A prefix of "" is
  # the common case and still narrows the grant to v1/.
  prefix       = trim(var.s3_prefix, "/")
  key_prefix   = local.prefix == "" ? "v1/" : "${local.prefix}/v1/"
  log_group    = "/ecs/${var.project}"
  container    = "ingester"
  metrics_args = var.metrics_enabled ? ["-metrics-namespace", var.metrics_namespace, "-metrics-interval", var.metrics_interval] : []

  # The command is the whole configuration of the ingester. It is here rather
  # than baked into the image so that a window or a format change is an apply,
  # not a rebuild.
  capture_args = concat([
    "-dir", var.capture_dir,
    "-window", var.capture_window,
    "-format", var.capture_format,
    "-log", "json",
    "-s3-bucket", aws_s3_bucket.captures.id,
    "-s3-prefix", var.s3_prefix,
  ], local.metrics_args)
}
