ECS Celery Autoscaler is a project that allows for scaling of ECS task infrastructure based on demand, 
rather than leaving it running.

This library is best suited for bursty/intermittent workloads, where workers have gaps between jobs.
See the deployment warning below for why a continuously busy queue is a poor fit.

> [!WARNING]
Celery Beat will not schedule tasks if the ECS service is scaled to 0. If your service uses Beat it is recommended to
either run Beat as a sidecar in your web service or as an independent service.

# Requirements
1. Task server running on ECS
2. Celery using a redis instance as the broker

# Recommendations
1. `worker_disable_prefetch = True` set on your Celery app. By default, Celery prefetches jobs from the queue 
    and holds them in reserve as it works through the current job. This does not allow for even distribution of jobs
    among the ecs tasks this library spins up for you.
    In Django you can enable this setting with
   ```
   CELERY_WORKER_DISABLE_PREFETCH = True
   ```
2. If your jobs are idempotent, `acks_late = True` and `reject_on_worker_lost = True` set on your Celery app. By 
   default, Celery acknowledges a job when received. This means the job will not be re-enqueued if it fails. 
   Queue infrastructure carries an inherent risk of lost jobs, and as long as your jobs are idempotent you can
   ensure they are retried on failure with these two settings.
In Django you can enable these settings with
   ```
   CELERY_TASK_ACKS_LATE = True
   CELERY_TASK_REJECT_ON_WORKER_LOST = True
   ```

# Setup
1. Add `ecs:DescribeServices`, `ecs:UpdateService`, `ecs:GetTaskProtection`, and `ecs:UpdateTaskProtection`
   as permissions to your ECS service's IAM policy. Terraform instructions are shown below.
2. Install the `ecs_celery_autoscaler` package into your application's codebase.
3. Construct a `EcsCeleryAutoscaler` and call `.install()` once at process startup, as top-level code in 
   whichever module defines your Celery `app`

```python
from ecs_celery_autoscaler import EcsCeleryAutoscaler, QueueDepthMetric

scaler = EcsCeleryAutoscaler(
    celery_app=app,
    ecs_cluster="my-ecs-cluster",
    ecs_service="my-celery-worker-service",
    aws_region="us-east-1",
    queue_name="celery",
    min_workers=0,
    max_workers=1,
    protection_expires_minutes=60,
    metric=QueueDepthMetric(tasks_per_worker=2),
)

scaler.install()
```

- `redis_client`: optional. By default, it's derived from `celery_app.conf.broker_url`, which covers a standard 
  `redis://` or `rediss://` broker URL. Pass this explicitly if your broker isn't reachable via `broker_url` alone
- `min_workers` / `max_workers`: the range `desiredCount` is scaled within. Defaults (`0`/`1`)
- `metric`: the `ScalingMetric` that decides the target worker count every time the autoscaler recomputes it.
  Built-in options:
  - `QueueDepthMetric(tasks_per_worker=1)`: scales to `ceil(outstanding_tasks / tasks_per_worker)`, where
    `outstanding_tasks` is the broker queue length plus in-flight tasks. Set `tasks_per_worker` to the number
    of worker processes each ECS task runs.
  - `QueueLatencyMetric(scale_up_threshold_seconds, scale_down_threshold_seconds, window_seconds=300, scale_up_step=1, scale_down_step=1)`:
    instead of reacting to queue length, scales based on how long tasks are waiting to be picked up.
    Scales up by `scale_up_step` if any task has waited longer than `scale_up_threshold_seconds`
    within the last `window_seconds`; scales down by `scale_down_step` if the longest wait in that window is under
    `scale_down_threshold_seconds`, or if no samples exist in the window at all (nothing has waited
    recently); otherwise holds steady. With zero workers running there's
    nothing to measure a wait with either; in that case it bootstraps one worker if the broker queue
    isn't empty, and hands off to normal latency-based scaling from there.
  - To define your own, subclass `ScalingMetric` and implement
    `target_worker_count(self, *, current: int, pending: int) -> tuple[int, dict]`.
- `protection_expires_minutes`: how long an ECS task scale-in protection grant lasts before it must be
  renewed (this library renews automatically in the background for tasks that run longer than this).
  Set this to at least your longest anticipated task duration. ECS can refuse renewal calls while a
  deployment or scale-in is blocked on a protected task (see the deployment note below), so the
  original grant — not renewal — is what has to cover a task for its full duration in that situation.
- After a worker releases its scale-in protection, it doesn't resume task consumption until it has
  confirmed via the ECS task metadata endpoint that ECS hasn't already decided to stop it (checked a
  few times, spaced out, since ECS can take a moment to record that decision after protection drops).
- `AUTOSCALING_ENABLED` (environment variable, default enabled): set to `False` to
  disable the library entirely

# Logging
This library logs via `logging.getLogger("ecs_celery_autoscaler")` and doesn't attach any handlers of
its own, so its output only shows up if your application's logging configuration reaches that logger.
Frameworks that disable loggers not explicitly listed in their config — e.g. Django's `LOGGING` setting,
which defaults `disable_existing_loggers` to `True` — will silently drop every message from this logger,
including scaling decisions, unless you add it explicitly.

For Django, add an entry to `LOGGING["loggers"]`:
```python
"ecs_celery_autoscaler": {
    "handlers": ["console"],
    "level": "INFO",
},
```

# How Does it Work?
This library utilizes Celery's signals along with redis statistics to track queue depth and each worker's processing state.

**How many workers:** on every task publish and task completion, the target worker count is recomputed by the
configured `metric` (see `metric` above), then clamped to `[min_workers, max_workers]` and to never go below the
number of currently busy workers. The ECS service is then scaled up/down if the target count differs from the
current count.

**Which worker is safe to remove:** Whenever a worker picks up a task it marks it as protected from scale-in via 
ECS Task Protection. On task completion, it removes the protection.

> [!WARNING]
> ECS honors task scale-in protection during deployments too — a rolling or blue/green deployment will not stop
> a protected task. If a deployment (or a manual `desiredCount` change) tries to converge below the number of
> currently protected tasks, ECS marks that convergence `DEPLOYMENT_BLOCKED` and will refuse *all*
> `UpdateTaskProtection` calls for the affected task until it's unblocked — including this library's own renewal
> calls. Avoid manually changing `desiredCount` on a service managed by this library while tasks may be in
> flight; let it own that value.
>
> Protection is only released when a worker goes idle, which this library only ever notices between jobs.
> If a queue keeps a worker continuously busy — the next job always lands before it goes idle — that worker
> never releases protection and stays `DEPLOYMENT_BLOCKED` indefinitely, making it effectively impossible to
> deploy new code while that load continues. This library is best suited for bursty/intermittent workloads with
> real gaps between jobs, not queues that keep a worker saturated.

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
