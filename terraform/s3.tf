# The capture bucket.
#
# force_destroy is false and stays false. A `terraform destroy` that quietly
# deleted every capture would make the teardown step — which this project runs
# between every session — the most dangerous command in the repo. Destroy is
# supposed to fail on a bucket with objects in it; emptying it is a separate,
# deliberate command, documented in README.md here.
resource "aws_s3_bucket" "captures" {
  bucket        = local.bucket_name
  force_destroy = false
}

# Versioning is off. Append-only is enforced where it can actually be enforced:
# every PutObject carries If-None-Match: "*", so S3 refuses to overwrite an
# existing key, and there is no code path in this project that deletes one.
# Versioning would pay to keep the history of overwrites that cannot happen.
resource "aws_s3_bucket_versioning" "captures" {
  bucket = aws_s3_bucket.captures.id

  versioning_configuration {
    status = "Disabled"
  }
}

# SSE-S3 rather than SSE-KMS: encrypted at rest either way, and AES256 costs
# nothing per request and needs no key grant in the task role. A KMS key would
# be a fourth service bought with a fourth IAM statement.
resource "aws_s3_bucket_server_side_encryption_configuration" "captures" {
  bucket = aws_s3_bucket.captures.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "captures" {
  bucket = aws_s3_bucket.captures.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "captures" {
  bucket = aws_s3_bucket.captures.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "captures" {
  bucket = aws_s3_bucket.captures.id

  # A capture is read hardest by the backtest that follows it. After a month it
  # is archive: IA is 45% cheaper to store and only costs on retrieval, and a
  # tape object is a whole window — hundreds of KB to a few MB — so it is well
  # clear of the 128 KB below which IA's minimum billable size loses money.
  rule {
    id     = "captures-to-ia"
    status = "Enabled"

    filter {}

    transition {
      days          = var.ia_transition_days
      storage_class = "STANDARD_IA"
    }
  }

  # Expiry is the one thing here that removes stored data, and it is opt-out.
  dynamic "rule" {
    for_each = var.expiration_days > 0 ? [1] : []

    content {
      id     = "captures-expire"
      status = "Enabled"

      filter {}

      expiration {
        days = var.expiration_days
      }
    }
  }

  # A multipart upload that died mid-flight is billed until it is aborted, and
  # is invisible in the object listing. This is the cheapest line in the file.
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.captures]
}
