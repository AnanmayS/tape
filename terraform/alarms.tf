resource "aws_sns_topic" "alarms" {
  name = "${local.name}-alarms"
}

# Optional. Without an address the topic still exists and the alarms still
# change state; there is just nobody on the other end. Confirm the subscription
# from the email AWS sends, or it stays pending and delivers nothing.
resource "aws_sns_topic_subscription" "email" {
  count = var.alarm_email == "" ? 0 : 1

  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# Invariant 2: gaps are never silent.
#
# The threshold is zero because the right number of gaps is zero. A gap means
# a reconnect lost frames the public feed will not sell back, so the window is
# untrustworthy and someone has to decide what to do about it. There is no
# "acceptable rate" to tune this to.
#
# Missing data is not breaching: between capture sessions this stack is
# deliberately at desired_count 0, and an alarm that screams whenever the
# ingester is off is an alarm people learn to ignore.
resource "aws_cloudwatch_metric_alarm" "gaps" {
  alarm_name        = "${local.name}-sequence-gap"
  alarm_description = "The ingester recorded a sequence gap. That window is untrustworthy: the public feed offers no backfill, so the gap record is the correction. See the tape file, not just this alarm."

  namespace   = var.metrics_namespace
  metric_name = "GapsDetected"
  dimensions = {
    Product = var.metrics_product
  }

  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}

# Ingest lag: the exchange's own timestamp to the moment the record was handed
# to the writer. It is a trend signal and not a latency SLO — it carries the
# exchange's clock skew and the internet between here and there — but it is the
# number that moves first when the reader stops keeping up with the socket,
# which is the failure M7 exists to characterise.
#
# Maximum, not Average: the interesting event is one record arriving late, and
# an average over a minute of 30 messages a second buries it.
resource "aws_cloudwatch_metric_alarm" "ingest_lag" {
  alarm_name        = "${local.name}-ingest-lag"
  alarm_description = "Exchange-timestamp-to-write lag stayed above the threshold for three minutes. Either the feed is behind or this task is not draining the socket fast enough."

  namespace   = var.metrics_namespace
  metric_name = "IngestLag"
  dimensions = {
    Product = var.metrics_product
  }

  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  threshold           = var.ingest_lag_threshold_seconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}
