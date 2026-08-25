# Deploying the ingester

The infrastructure in [`terraform/`](../terraform) is meant to be created for a
capture session and destroyed after it. It is not a standing production
environment and nothing in this document assumes it is.

Everything here is a runbook, not a claim. Nothing in it has been applied yet;
see [What has not been verified](#what-has-not-been-verified) at the end for the
list of things that only a real apply can settle.

## What gets created

| | |
|---|---|
| S3 | One bucket. Unversioned, SSE-S3, public access blocked, IA at 30 days, expiry at a year. `force_destroy = false`. |
| ECR | One repository for the image, keeping the last 10. `force_delete = true`. |
| ECS | One cluster, one task definition, one Fargate service at `desired_count = 1`. 0.25 vCPU, 512 MB. |
| CloudWatch | One log group (14-day retention), five custom metrics, two alarms. |
| SNS | One topic for the alarms, with an optional email subscription. |
| IAM | Two roles: the standard execution role, and a task role that may `PutObject` under one prefix and publish to one metric namespace. |
| VPC | None. The task runs in the account's default VPC with a public IP and an egress-only security group. |

Three AWS services do the work — S3, ECS Fargate, CloudWatch. ECR and IAM are
there because a container has to come from somewhere and run as someone.

There is no NAT gateway, and there should not be. A private subnet would cost
about $32 a month to hide a task that accepts no inbound connections at all.

## What it costs

These are AWS list prices for `us-east-1` at the time of writing, not numbers
off a bill. Treat them as an order of magnitude.

| | per hour running | per month if left up |
|---|---|---|
| Fargate, 0.25 vCPU + 0.5 GB | ~$0.012 | ~$9 |
| Public IPv4 address | $0.005 | ~$3.60 |
| 5 custom metrics | — | ~$1.50 |
| 2 alarms | — | ~$0.20 |
| S3 and logs for a day of BTC-USD | — | pennies |

About $0.40 a day while capturing. The point of the pause and teardown steps
below is that the first two rows — which are all of it — stop the moment the
task does.

The feed is free and stays free. Nothing in this project requires paid market
data.

## Prerequisites

- Terraform >= 1.9 and the AWS CLI.
- Credentials for an account you are willing to create and destroy things in.
- Docker, to build the image.

There is no `.terraform.lock.hcl` in the repository, because `terraform init`
has never been run against this configuration. The first `init` writes one;
commit it, so that every later apply resolves the same provider build rather
than whatever `~> 6.0` means that week.

## The lifecycle

### 1. Create the infrastructure, paused

The service needs an image and the image needs somewhere to be pushed, so the
first apply creates everything with no task running:

```
terraform -chdir=terraform init
terraform -chdir=terraform apply -var desired_count=0
```

Read the plan. It should be about twenty resources and no surprises.

If you want alarm email, add `-var alarm_email=you@example.com` and confirm the
subscription from the message AWS sends. An unconfirmed subscription delivers
nothing and says so in the console.

### 2. Build and push the image

```
REGION=$(terraform -chdir=terraform output -raw aws_region)
REPO=$(terraform -chdir=terraform output -raw ecr_repository_url)

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${REPO%%/*}"

docker build -t "$REPO:latest" .
docker push "$REPO:latest"
```

The GitHub Actions workflow in
[`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) does the same
push from CI, but it is manual-dispatch only and refuses to run until an OIDC
role and two repository variables exist. The first image goes up by hand
regardless, because the repository has to exist before anything can push to it.

### 3. Start capturing

```
terraform -chdir=terraform apply
```

That moves `desired_count` to 1 and the task starts. Watch it come up:

```
aws logs tail "$(terraform -chdir=terraform output -raw log_group)" --follow
```

The first useful line is `capture starting`, and the second is `rotated` at the
end of the first window.

### 4. Verify the capture is real

Three checks, in order of how much they prove.

**Objects are landing.** After one rotation window — five minutes by default —
there should be objects under the layout prefix:

```
BUCKET=$(terraform -chdir=terraform output -raw bucket_name)
aws s3 ls "s3://$BUCKET/v1/symbol=BTC-USD/" --recursive
```

**The window reads back.** `verify` replays it and exits non-zero if it contains
a gap or a reconnect:

```
go build -o tape ./cmd/tape
./tape verify -s3-bucket "$BUCKET" v1/symbol=BTC-USD/date=$(date -u +%Y-%m-%d)
```

**Replay is still deterministic out of the bucket.** This is the invariant, and
it is worth checking against real captured data rather than only against the
fixture in the test suite:

```
./tape replay -s3-bucket "$BUCKET" v1/symbol=BTC-USD/date=$(date -u +%Y-%m-%d) | sha256sum
./tape replay -s3-bucket "$BUCKET" v1/symbol=BTC-USD/date=$(date -u +%Y-%m-%d) | sha256sum
```

Two identical digests, or the deployment has broken the one property this
project exists for.

**Metrics are arriving.** Give it two intervals, then:

```
aws cloudwatch list-metrics --namespace "$(terraform -chdir=terraform output -raw metrics_namespace)"
```

Five metric names, each with a single `Product` dimension. If the dimension is
missing or different, the alarms are watching a metric nothing writes — which
looks exactly like a healthy system.

### 5. Pause between sessions

```
terraform -chdir=terraform apply -var desired_count=0
```

The task stops and the Fargate and public-IP charges stop with it. Everything
else — the bucket, the captures, the alarms, the image — stays. This is the
right move between sessions on the same data; teardown is for the end of one.

### 6. Tear it down

```
terraform -chdir=terraform destroy
```

**This will fail if the bucket has objects in it, and that is the design.**
`force_destroy` is `false`. A teardown command that quietly deleted every
capture would be the most dangerous line in this repository, and it is run
often.

So the deliberate order is: get the data out, then empty, then destroy.

```
# 1. Copy the captures somewhere you actually want them.
aws s3 sync "s3://$BUCKET/v1/" ./data/v1/

# 2. Check you got them. Byte counts, not vibes.
aws s3 ls "s3://$BUCKET/v1/" --recursive --summarize | tail -3
find ./data/v1 -name '*.tape' | wc -l

# 3. Replay the local copy and confirm it is the same window you verified above.
./tape replay ./data/v1/symbol=BTC-USD/date=YYYY-MM-DD | sha256sum

# 4. Only now.
aws s3 rm "s3://$BUCKET/v1/" --recursive
terraform -chdir=terraform destroy
```

Step 3 is not ceremony. A `sync` that silently dropped an object leaves a window
that replays fine and is missing a minute, and the digest is the only thing that
would have noticed.

### What destroy does and does not remove

Removed: the cluster, service and task definition; the log group and everything
in it; the alarms and the SNS topic; both IAM roles; the security group; the ECR
repository **and its images** (`force_delete = true` — an image is a build
artifact and rebuilding it is a `docker build`).

Not removed: the objects in the bucket, because destroy refuses to run while
they exist. Also not removed, because Terraform never knew about them: anything
you or the console put in the bucket outside the `v1/` prefix, and any email
subscription confirmation state.

Left behind on your machine: `terraform.tfstate`. It is gitignored and it is how
the next destroy knows what to remove. Do not delete it while resources exist.

### Confirming nothing is still billing

```
aws ecs list-clusters
aws s3 ls | grep tape
aws cloudwatch describe-alarms --alarm-name-prefix tape
aws ecr describe-repositories
aws logs describe-log-groups --log-group-name-prefix /ecs/tape
```

All five should come back empty. The public IPv4 charge is attached to the task,
so it goes when the task does; there is no Elastic IP in this stack to strand.

## Operating notes

**Redeploying is a hole in the tape.** The service is configured
`minimum_healthy_percent = 0` / `maximum_percent = 100`, so the old task stops
before the new one starts. That is deliberate: the alternative overlaps two
ingesters on one feed and stores every frame twice under two keys. The gap
shows up honestly as a reseed record, and as a gap record if the sequence moved
while nothing was listening.

**The alarms do not notice a stopped ingester.** Both treat missing data as not
breaching, because this stack sits at `desired_count = 0` between sessions by
design and an alarm that fires every time it is deliberately off is an alarm
people stop reading. What covers a task that died rather than one that was
stopped is the ECS service restarting it. If this deployment ever runs
unattended for long enough that the difference matters, the fix is an alarm on
`MessagesReceived` with `treat_missing_data = "breaching"` and a maintenance
switch — and it should be added then, on that need, not now.

**On Fargate, S3 is the durable copy.** Everywhere else in this project local
disk is durable and the upload is a convenience. Here `-dir` is ephemeral task
storage that disappears with the task, so an upload that runs out of attempts is
a real loss. That is why the uploader logs one at error level and why
`upload_failed` in the session summary is worth reading.

**Configuration is in the task definition, not the image.** The entrypoint is
`tape capture` and every flag comes from `local.capture_args` in
[`main.tf`](../terraform/main.tf). Changing the rotation window or switching to
the columnar format is an apply, not a rebuild.

**Switching to ARM64** means setting `cpu_architecture = "ARM64"` *and* building
the image for it (`docker buildx build --platform linux/arm64`). Fargate rejects
a task whose image does not match the platform in its runtime configuration, and
the failure appears as a task that will not start rather than as anything about
architecture.

## What has not been verified

Honest accounting of what is written but not yet proven, because this stack has
never been applied:

- **No `terraform plan` or `apply` has been run.** CI runs `fmt -check`, `init
  -backend=false` and `validate`, which catch syntax, type and reference errors.
  They cannot catch a resource AWS rejects at create time, an IAM policy that is
  valid but too narrow, or a default VPC that does not exist in the account.
- **The task role has not been proven sufficient.** `s3:PutObject` with no
  `ListBucket` and no `GetObject` is what the capture path should need, and it
  is what the code does; the proof is a capture landing objects under that role.
- **The gap alarm has never fired.** The path from a sequence gap to an email
  is: capture increments a counter, the collector folds it, `PutMetricData`
  sends it under `Product=BTC-USD`, the alarm matches that dimension, SNS
  delivers. Each link is tested or asserted in isolation. End to end, it is
  untested. The honest check is to induce a gap on a real deployment — sever the
  connection, as the M2 measurements did — and watch for the notification.
- **The image has not been built.** The Dockerfile is written and both the amd64
  and arm64 static builds succeed locally, but the machine this was written on
  cannot reach a Docker daemon. The CI `docker` job is the first real build.
- **Cost figures above are list prices, not a bill.**
