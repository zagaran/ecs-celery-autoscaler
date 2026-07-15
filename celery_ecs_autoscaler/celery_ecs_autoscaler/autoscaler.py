from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import boto3
from celery.signals import after_task_publish, task_postrun

log = logging.getLogger("celery_ecs_autoscaler")


class CeleryEcsAutoscaler:
    """Scales one ECS service's desiredCount between 0 and 1 based on Celery
    task publish/completion.
    """

    def __init__(
        self,
        *,
        celery_app,
        redis_client,
        ecs_cluster: str,
        ecs_service: str,
        aws_region: str,
        queue_name: str = "celery",
        inspect_timeout: float = 1,
        inspect_retries: int = 10,
        inspect_retry_delay: float = 3,
        ecs_client: Any = None,
    ):
        self.celery_app = celery_app
        self.redis_client = redis_client
        self.ecs_cluster = ecs_cluster
        self.ecs_service = ecs_service
        self.queue_name = queue_name
        self.inspect_timeout = inspect_timeout
        self.inspect_retries = inspect_retries
        self.inspect_retry_delay = inspect_retry_delay
        self._ecs = ecs_client or boto3.client("ecs", region_name=aws_region)

    def install(self) -> None:
        """Wire the signal handlers"""
        after_task_publish.connect(self._on_publish, weak=False)
        task_postrun.connect(self._on_postrun, weak=False)

    def _on_publish(self, sender=None, routing_key=None, **kwargs):
        if routing_key == self.queue_name:
            self.scale_up()

    def _on_postrun(self, sender=None, task_id=None, **kwargs):
        """Runs the scale down check in a background thread. This is necessary in the case that Celery runs with
        multiple workers. The scale down chack should not block the other worker considering this task to be
        completed.
        """
        threading.Thread(target=self.maybe_scale_down, kwargs={"exclude_task_id": task_id}, daemon=True).start()

    def _desired_count(self) -> int:
        resp = self._ecs.describe_services(cluster=self.ecs_cluster, services=[self.ecs_service])
        return resp["services"][0]["desiredCount"]

    def scale_up(self) -> None:
        """Safe to call unconditionally and often — a no-op once desiredCount is already 1."""
        try:
            if self._desired_count() == 0:
                self._ecs.update_service(cluster=self.ecs_cluster, service=self.ecs_service, desiredCount=1)
                log.info("scaled %s up to 1", self.ecs_service)
        except Exception:
            log.exception("scale_up failed for %s", self.ecs_service)

    def maybe_scale_down(self, exclude_task_id: str | None = None) -> None:
        """Scales to 0 only if no worker reports an active task AND the queue is
        empty. If active-task state still can't be determined after retries
        (broker unreachable, no worker replied), assumes busy and does nothing.

        exclude_task_id: the task that just triggered this check via
        task_postrun. Celery's worker removes a request from its active-task
        bookkeeping only after task_postrun fires, so control.inspect().active()
        can still list the just-finished task as active at this exact instant.
        """
        try:
            active = None
            for attempt in range(self.inspect_retries):
                active = self._active_tasks(exclude_task_id)
                if active is not None:
                    break
                if attempt < self.inspect_retries - 1:
                    time.sleep(self.inspect_retry_delay)
            if active is None:
                log.info("active-task state unknown after %d attempts, not scaling down", self.inspect_retries)
                return
            if active:
                log.info(
                    "not scaling down %s, exclude_task_id=%r, active tasks reported: %r",
                    self.ecs_service,
                    exclude_task_id,
                    active,
                )
                return
            queue_len = self.redis_client.llen(self.queue_name)
            if queue_len > 0:
                log.info("not scaling down %s, %d message(s) still on queue %r", self.ecs_service, queue_len, self.queue_name)
                return
            if self._desired_count() > 0:
                self._ecs.update_service(cluster=self.ecs_cluster, service=self.ecs_service, desiredCount=0)
                log.info("scaled %s down to 0", self.ecs_service)
        except Exception:
            log.exception("maybe_scale_down failed for %s", self.ecs_service)

    def _active_tasks(self, exclude_task_id: str | None = None) -> list | None:
        """Retrieves a list of tasks being processed by the Celery worker(s).
        This includes active, reserved, and scheduled tasks.
        """
        try:
            inspector = self.celery_app.control.inspect(timeout=self.inspect_timeout)
            with ThreadPoolExecutor(max_workers=3) as executor:
                # Run the three inspector checks concurrently
                active_future = executor.submit(inspector.active)
                reserved_future = executor.submit(inspector.reserved)
                scheduled_future = executor.submit(inspector.scheduled)
                active_result = active_future.result()
                reserved_result = reserved_future.result()
                scheduled_result = scheduled_future.result()
        except Exception:
            log.exception("broker unreachable, cannot inspect active/reserved/scheduled tasks")
            return None
        if active_result is None or reserved_result is None or scheduled_result is None:
            return None
        tasks = [task for tasks in active_result.values() for task in tasks]
        tasks += [task for tasks in reserved_result.values() for task in tasks]
        tasks += [
            entry["request"]
            for entries in scheduled_result.values()
            for entry in entries
        ]
        if exclude_task_id is not None:
            tasks = [t for t in tasks if t.get("id") != exclude_task_id]
        return tasks