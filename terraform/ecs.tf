resource "aws_cloudwatch_log_group" "ingester" {
  name              = local.log_group
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_cluster" "main" {
  name = local.name

  # Container Insights is off. It is a per-task-per-month charge for CPU and
  # memory graphs of a task that is neither CPU- nor memory-bound; the numbers
  # this project needs are the ones the ingester publishes itself.
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

# Egress only. The ingester dials out to Coinbase, S3 and CloudWatch and
# accepts nothing, so there is no ingress rule to write — which is also why
# there is no private subnet and no NAT gateway behind it.
resource "aws_security_group" "task" {
  name        = "${local.name}-task"
  description = "Tape ingester: outbound only"
  vpc_id      = local.vpc_id
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.task.id
  description       = "wss:// to the exchange, HTTPS to S3 and CloudWatch"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_ecs_task_definition" "ingester" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([
    {
      name      = local.container
      image     = "${aws_ecr_repository.ingester.repository_url}:${var.image_tag}"
      essential = true

      # The image's entrypoint is `tape capture`; these are its flags.
      command = local.capture_args

      environment = [
        # Fargate hands the task role's credentials to the SDK through the
        # container credentials endpoint, but it does not hand it a region.
        # Without this, every S3 and CloudWatch call fails to resolve an
        # endpoint and the ingester captures to ephemeral disk and nowhere else.
        {
          name  = "AWS_REGION"
          value = var.aws_region
        },
      ]

      # SIGTERM is a clean stop: the writer drains its channel, flushes, closes
      # the open window and the uploader drains its queue. That last part is
      # why this is 60s and not the 30s default — the drain itself is 30s, and
      # a SIGKILL through the middle of it strands a window on a disk that is
      # about to disappear.
      stopTimeout = 60

      # A crash restarts the container in place. The service below replaces the
      # task if that stops working, but an in-place restart costs seconds
      # instead of a minute of missed frames.
      restartPolicy = {
        enabled              = true
        restartAttemptPeriod = 300
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ingester.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = local.container
        }
      }
    },
  ])
}

resource "aws_ecs_service" "ingester" {
  name            = local.name
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.ingester.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  # Stop the old task before starting the new one. The usual default — start
  # the replacement first — would put two ingesters on the same feed at once,
  # and two captures of the same window is two copies of every frame under two
  # keys. A few seconds of gap is honest; it shows up as a reseed and, if the
  # sequence moved, a gap record. Duplicate windows would not show up at all.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  network_configuration {
    subnets         = local.subnet_ids
    security_groups = [aws_security_group.task.id]

    # No NAT gateway in this stack, so the task needs its own public IP to
    # reach the exchange. Nothing can reach it: the security group has no
    # ingress rule.
    assign_public_ip = true
  }

  # The service is what restarts a task that exited for good. There is no
  # load balancer and no health check beyond "the process is alive", because
  # a WebSocket reader that is alive is a WebSocket reader that is working —
  # and if it is not, the gap alarm says so.
}
