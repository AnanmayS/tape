terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # State is local on purpose. A remote backend needs a bucket and a lock table
  # that exist before the first apply, and this stack is one engineer applying
  # and destroying it between capture sessions. terraform.tfstate is gitignored;
  # keep it, because destroy needs it.
}
