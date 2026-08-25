variable "aws_region" {
  description = "Region everything lives in. One region; there is no multi-region story here."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Name prefix for every resource, and the value of the Project tag."
  type        = string
  default     = "tape"
}

variable "bucket_name" {
  description = <<-EOT
    Capture bucket name. Empty derives "<project>-captures-<account id>-<region>",
    which is globally unique without an account id being written into this repo.
  EOT

  type    = string
  default = ""
}

variable "s3_prefix" {
  description = <<-EOT
    Key prefix inside the bucket, passed to `tape capture -s3-prefix`. Empty
    means keys start at the layout version: "v1/symbol=.../date=...". The task
    role's PutObject grant is scoped to this prefix plus "v1/" and nothing else.
  EOT

  type    = string
  default = ""
}

variable "ia_transition_days" {
  description = <<-EOT
    Age at which captures move to STANDARD_IA. A tape object is a whole window,
    hundreds of KB to a few MB, so it is clear of the 128 KB below which IA's
    minimum billable size costs more than it saves.
  EOT

  type    = number
  default = 30
}

variable "expiration_days" {
  description = <<-EOT
    Age at which captures are deleted. This is the only thing in the stack that
    removes stored data, which is why invariant 3 is about never rewriting an
    object rather than about keeping every byte forever. 0 disables expiry.
  EOT

  type    = number
  default = 365
}

variable "ecr_keep_images" {
  description = "How many tagged images the ECR repository keeps."
  type        = number
  default     = 10
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for the ingester's log group."
  type        = number
  default     = 14
}

variable "task_cpu" {
  description = <<-EOT
    Fargate CPU units. 256 (0.25 vCPU) is the floor and is the right size: this
    is one WebSocket reader, one file writer and one uploader. Raise it on a
    measured number, not on a hunch.
  EOT

  type    = number
  default = 256
}

variable "task_memory" {
  description = "Fargate memory in MiB. 512 is the smallest value 256 CPU units allow."
  type        = number
  default     = 512
}

variable "cpu_architecture" {
  description = <<-EOT
    Fargate CPU architecture. ARM64 is about 20% cheaper and Go builds for it
    without complaint, but the image has to be built for it too — see
    docs/deploy.md before changing this.
  EOT

  type    = string
  default = "X86_64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.cpu_architecture)
    error_message = "cpu_architecture must be \"X86_64\" or \"ARM64\"."
  }
}

variable "desired_count" {
  description = <<-EOT
    Tasks to run. One ingester, so 1 or 0. Setting it to 0 and applying stops
    the Fargate charge without destroying anything — that is the pause between
    capture sessions; `terraform destroy` is the end of one.
  EOT

  type    = number
  default = 1

  validation {
    condition     = var.desired_count == 0 || var.desired_count == 1
    error_message = "There is one ingester. desired_count is 0 (paused) or 1 (running)."
  }
}

variable "image_tag" {
  description = "Tag in the ECR repository that the task definition runs."
  type        = string
  default     = "latest"
}

variable "capture_window" {
  description = "Value for `tape capture -window`: the file rotation window."
  type        = string
  default     = "5m"
}

variable "capture_format" {
  description = <<-EOT
    Value for `tape capture -format`: "raw" (v1 record log) or "columnar" (v2,
    5.1x smaller). raw is the binary's default until M7 measures the columnar
    writer under load; see the M5 section of README.md.
  EOT

  type    = string
  default = "raw"

  validation {
    condition     = contains(["raw", "columnar"], var.capture_format)
    error_message = "capture_format must be \"raw\" or \"columnar\"."
  }
}

variable "capture_dir" {
  description = <<-EOT
    Container path for the local copy. On Fargate this is ephemeral task
    storage: it lives exactly as long as the task does. On this deployment the
    bucket is therefore the durable copy, and an upload that runs out of
    attempts is a real loss rather than a re-run — which is why the uploader
    logs one at error level and why the alarms below exist.
  EOT

  type    = string
  default = "/tmp/tape"
}

variable "metrics_enabled" {
  description = <<-EOT
    Publish CloudWatch metrics from the ingester. Off by default in the binary
    so that a local capture never needs AWS; on here, because the alarms are
    most of the reason this deployment exists.
  EOT

  type    = bool
  default = true
}

variable "metrics_namespace" {
  description = <<-EOT
    CloudWatch namespace the ingester publishes to. The task role may write to
    this namespace and no other, enforced by an IAM condition key, so changing
    it here changes both the emitter's flag and the grant.
  EOT

  type    = string
  default = "Tape"
}

variable "metrics_product" {
  description = <<-EOT
    Value of the Product dimension the alarms match on. It has to equal what
    the ingester captures — v1 is one exchange and one product — or the alarms
    watch a metric nothing writes and never fire.
  EOT

  type    = string
  default = "BTC-USD"
}

variable "metrics_interval" {
  description = "How often the ingester aggregates and publishes: one PutMetricData call per interval."
  type        = string
  default     = "60s"
}

variable "ingest_lag_threshold_seconds" {
  description = <<-EOT
    Alarm threshold on the maximum exchange-timestamp-to-write delay. Coinbase
    stamps its own clock, so this carries clock skew and is a signal about
    trend rather than a latency SLO.
  EOT

  type    = number
  default = 5
}

variable "alarm_email" {
  description = "Address subscribed to the alarm topic. Empty creates the topic with no subscription."
  type        = string
  default     = ""
}

variable "vpc_id" {
  description = "VPC for the task. Empty uses the account's default VPC."
  type        = string
  default     = ""
}

variable "subnet_ids" {
  description = <<-EOT
    Subnets for the task. Empty uses the default VPC's subnets. They need a
    route to the internet: the task takes a public IP and talks straight out to
    Coinbase, S3 and CloudWatch. There is no NAT gateway in this stack and
    there should not be — it would cost more per month than everything else
    here put together.
  EOT

  type    = list(string)
  default = []
}
