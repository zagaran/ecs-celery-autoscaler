ECS Celery Autoscaler is a project that allows for scaling of ECS task infrastructure based on demand, 
rather than leaving it running.

# Requirements
1. Task server running on ECS
2. Celery using an Elasticache redis instance as the broker

# Setup
1. Add `ecs:DescribeServices` and `ecs:UpdateService` as permissions to your ECS service's IAM policy. 
   Terraform instructions are shown below.
2. Install the `celery_ecs_autoscaler` package into your application's codebase.
3. Construct a `CeleryEcsAutoscaler` and call `.install()` once at process startup, as top-level code in 
   whichever module defines your Celery `app`
```python
from celery_ecs_autoscaler import CeleryEcsAutoscaler

scaler = CeleryEcsAutoscaler(
    celery_app=app,
    redis_client=redis.Redis(host="...", decode_responses=True),
    ecs_cluster="my-ecs-cluster",
    ecs_service="my-celery-worker-service",
    aws_region="us-east-1",
    queue_name="celery",
)

scaler.install()
```

# How Does it Work?
This library utilizes Celery's signals to check the state of Celery's workers and the redis queue.

Your ECS service is scaled to 0:
- When a task is published to the queue, the `after_task_publish` signal fires and scales the service up to 1.

Your ECS service is scaled to 1:
- When a task finishes the `task_postrun` signal fires and checks whether any worker
  still reports any active, scheduled, or reserved tasks and whether the queue is empty. If all are clear, it scales the service down
  to 0.

# Terraform Instructions
1. Ensure that your aws cli is pointed to the desired AWS account
2. Clone this repo, and add a `mgmt.tfvars` to `/infrastructure` based on `mgmt.tfvars.example`
3. Create an s3 bucket to hold the terraform state
4. Initialize terraform
```bash
terraform init -backend-config="bucket=BUCKET_NAME" -backend-config="region=REGION" -backend-config="key=KEY"
```
5. Apply terraform. This creates an IAM policy scoped to your ECS service (`ecs:DescribeServices` /
   `ecs:UpdateService`) and attaches it to the IAM role you name in `ecs_task_role` — the role shared
   by both the publisher and the consumer tasks.
```bash
terraform apply -var-file=mgmt.tfvars
```
