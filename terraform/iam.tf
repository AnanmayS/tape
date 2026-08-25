# Two roles, and the difference between them is the point.
#
# The execution role is ECS's: it pulls the image and opens the log stream
# before the container exists. The task role is the ingester's, and it is the
# one that matters, because it is what a bug or a compromised dependency inside
# the process can actually use.

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

# The whole of what the ingester may do.
#
# s3:PutObject on one prefix of one bucket, and nothing else. No GetObject: the
# capture path never reads an object back. No DeleteObject: nothing in this
# project deletes a capture, and a role that cannot delete one is a stronger
# statement of that than a code review is. No ListBucket: the conditional put
# needs no listing, and replay — which does list — runs from a workstation
# under a human's credentials, not from this task.
#
# cloudwatch:PutMetricData takes no resource ARN, so the namespace condition is
# the only thing standing between "publish our five metrics" and "write into
# any namespace in the account, including AWS/ECS". It is not optional.
data "aws_iam_policy_document" "task" {
  statement {
    sid       = "PutCapturesOnly"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.captures.arn}/${local.key_prefix}*"]
  }

  statement {
    sid       = "PublishOwnMetricsOnly"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = [var.metrics_namespace]
    }
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "${local.name}-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}
