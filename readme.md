ECS Celery Autoscaler is a project that allows for scaling of ECS task infrastructure based on demand, 
rather than leaving it running.

> [!WARNING]
This package currently does not work for scheduled tasks. They will not run if your service is scaled to 0.

# Requirements
1. Task server running on ECS
2. Celery using a redis instance as the broker
3. `task_acks_late = True` set on your Celery app — strongly recommended so that a task killed
   mid-flight (by a deploy, spot interruption, crash, etc.) is redelivered instead of lost.

# Setup
1. Add `ecs:DescribeServices`, `ecs:UpdateService`, `ecs:GetTaskProtection`, and `ecs:UpdateTaskProtection`
   as permissions to your ECS service's IAM policy. Terraform instructions are shown below.
2. Install the `ecs_celery_autoscaler` package into your application's codebase.
3. Construct a `EcsCeleryAutoscaler` and call `.install()` once at process startup, as top-level code in 
   whichever module defines your Celery `app`

```python
from ecs_celery_autoscaler import EcsCeleryAutoscaler

scaler = EcsCeleryAutoscaler(
    celery_app=app,
    redis_client=redis.Redis(host="...", decode_responses=True),
    ecs_cluster="my-ecs-cluster",
    ecs_service="my-celery-worker-service",
    aws_region="us-east-1",
    queue_name="celery",
    min_workers=0,
    max_workers=1,
    tasks_per_worker=1,
    protection_expires_minutes=60,
)

scaler.install()
```

- `min_workers` / `max_workers`: the range `desiredCount` is scaled within. Defaults (`0`/`1`) match
  this library's original scale-to-zero-or-one behavior exactly.
- `tasks_per_worker`: target worker count is `ceil(queue_length / tasks_per_worker)`, clamped to
  `[min_workers, max_workers]`.
- `protection_expires_minutes`: how long an ECS task scale-in protection grant lasts before it must be
  renewed (this library renews automatically in the background for tasks that run longer than this).

# How Does it Work?
This library utilizes Celery's signals to track queue depth and each worker's busy/idle state.

**How many workers:** on every task publish (`after_task_publish`) and task completion
(`task_postrun`), the target worker count is recomputed from the current Redis queue length
(`ceil(queue_length / tasks_per_worker)`, clamped to `[min_workers, max_workers]`), and `desiredCount`
is updated to match if it differs.

**Which worker is safe to remove:** this library does not decide that itself. Instead, each worker
marks its own ECS task as protected from scale-in (via the
[ECS task scale-in protection endpoint](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection-endpoint.html))
whenever it has any task running (`task_prerun`) and unprotects itself once idle (`task_postrun`).
ECS's own scale-in logic guarantees it will never terminate a protected (busy) task when reducing
`desiredCount` — it simply skips protected tasks and retries later. This means the library never needs
to broadcast-inspect the worker fleet or guess who's busy.

# Terraform Instructions
1. Ensure that your aws cli is pointed to the desired AWS account
2. Clone this repo, and add a `mgmt.tfvars` to `/infrastructure` based on `mgmt.tfvars.example`
3. Create an s3 bucket to hold the terraform state
4. Initialize terraform
```bash
terraform init -backend-config="bucket=BUCKET_NAME" -backend-config="region=REGION" -backend-config="key=KEY"
```
5. Apply terraform. This creates an IAM policy scoped to your ECS service (`ecs:DescribeServices` /
   `ecs:UpdateService`) and your cluster's tasks (`ecs:GetTaskProtection` / `ecs:UpdateTaskProtection`),
   and attaches it to the IAM role you name in `ecs_task_role` — the role shared by both the publisher
   and the consumer tasks.
```bash
terraform apply -var-file=mgmt.tfvars
```
