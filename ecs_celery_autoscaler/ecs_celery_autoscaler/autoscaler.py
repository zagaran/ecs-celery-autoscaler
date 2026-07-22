from __future__ import annotations

import json
import logging
import math
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any

import boto3
from celery.signals import after_task_publish, task_postrun, task_received

log = logging.getLogger("ecs_celery_autoscaler")

LOCK_TIMEOUT = 30
LOCK_BLOCKING_TIMEOUT = 10


class EcsCeleryAutoscaler:
    """Scales one ECS service's desiredCount across 0-max_workers based on Celery
    queue depth. Safety against killing a busy worker is delegated to ECS task
    scale-in protection rather than handled by this class.
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
        min_workers: int = 0,
        max_workers: int = 1,
        tasks_per_worker: int = 1,
        protection_expires_minutes: int = 60,
        ecs_client: Any = None,
    ):
        self.celery_app = celery_app
        self.redis_client = redis_client
        self.ecs_cluster = ecs_cluster
        self.ecs_service = ecs_service
        self.queue_name = queue_name
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.tasks_per_worker = tasks_per_worker
        self.protection_expires_minutes = protection_expires_minutes
        self.enabled = os.environ.get("AUTOSCALING_ENABLED", "True") not in ("FALSE", "False", "false")
        self._ecs = ecs_client or boto3.client("ecs", region_name=aws_region)
        self._lock_key = f"ecs-celery-autoscaler:{self.ecs_service}:lock"
        self._active_count_key = f"ecs-celery-autoscaler:{self.ecs_service}:active-count:{socket.gethostname()}"
        self._pending_count_key = f"ecs-celery-autoscaler:{self.ecs_service}:pending-count"

    def install(self) -> None:
        """Wire the signal handlers and start the protection renewal heartbeat."""
        if not self.enabled:
            log.warning("AUTOSCALING_ENABLED is false — %s autoscaler is disabled", self.ecs_service)
            return
        after_task_publish.connect(self._on_publish, weak=False)
        task_received.connect(self._on_received, weak=False)
        task_postrun.connect(self._on_postrun, weak=False)
        if not self.celery_app.conf.worker_disable_prefetch:
            log.warning(
                "worker_disable_prefetch is not enabled — workers that are already connected can "
                "prefetch tasks faster than they can run them, starving newly scaled-up workers of "
                "work. Strongly recommended for any scale-to-zero/N deployment."
            )
        threading.Thread(target=self._protection_renewal_loop, daemon=True).start()

    def _on_publish(self, sender=None, routing_key=None, **kwargs):
        if routing_key == self.queue_name:
            self.scale_up()

    def _on_received(self, request=None, **kwargs):
        """Fires the instant a task is delivered to this worker — before it necessarily has a free
        pool slot to run in. Protection and the pending count must both start here rather than at
        task_prerun, otherwise a task that's been delivered but is still waiting for a slot isn't
        counted as outstanding work, and its container can be stopped out from under it.
        """
        self.redis_client.incr(self._pending_count_key)
        if self.redis_client.incr(self._active_count_key) == 1:
            self._set_protection(True)

    def _on_postrun(self, task_id=None, **kwargs):
        if self.redis_client.decr(self._pending_count_key) <= 0:
            self.redis_client.set(self._pending_count_key, 0)
        if self.redis_client.decr(self._active_count_key) <= 0:
            self.redis_client.set(self._active_count_key, 0)
            self._set_protection(False)
        threading.Thread(target=self.maybe_scale_down, daemon=True).start()

    def scale_up(self) -> None:
        """Safe to call unconditionally and often — a no-op once desiredCount already meets target."""
        self._reconcile()

    def maybe_scale_down(self) -> None:
        """Recomputes the target worker count and scales toward it. Safe even if a worker is
        mid-task: ECS task scale-in protection (set via task_received/task_postrun) guarantees a busy
        task is never terminated, so this method doesn't need to know who's busy.
        """
        self._reconcile()

    def _reconcile(self) -> None:
        if not self.enabled:
            return
        try:
            with self.redis_client.lock(self._lock_key, timeout=LOCK_TIMEOUT, blocking_timeout=LOCK_BLOCKING_TIMEOUT):
                queue_len = self.redis_client.llen(self.queue_name)
                pending = int(self.redis_client.get(self._pending_count_key) or 0)
                outstanding = queue_len + pending
                target = self._target_worker_count(outstanding)
                current = self._desired_count()
                if target != current:
                    self._ecs.update_service(cluster=self.ecs_cluster, service=self.ecs_service, desiredCount=target)
                    log.info(
                        "reconciled %s to desiredCount=%d (queue_len=%d, pending=%d)",
                        self.ecs_service,
                        target,
                        queue_len,
                        pending,
                    )
        except Exception:
            log.exception("reconcile failed for %s", self.ecs_service)

    def _target_worker_count(self, outstanding: int) -> int:
        return min(self.max_workers, max(self.min_workers, math.ceil(outstanding / self.tasks_per_worker)))

    def _desired_count(self) -> int:
        resp = self._ecs.describe_services(cluster=self.ecs_cluster, services=[self.ecs_service])
        return resp["services"][0]["desiredCount"]

    def _set_protection(self, enabled: bool) -> bool:
        """Returns whether protection was confirmed set to `enabled`."""
        agent_uri = os.environ.get("ECS_AGENT_URI")
        if not agent_uri:
            return True
        body: dict[str, Any] = {"ProtectionEnabled": enabled}
        if enabled:
            body["ExpiresInMinutes"] = self.protection_expires_minutes
        try:
            req = urllib.request.Request(
                f"{agent_uri}/task-protection/v1/state",
                data=json.dumps(body).encode(),
                method="PUT",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                parsed = json.loads(resp.read())
            if "error" in parsed or "failure" in parsed:
                raise RuntimeError(parsed.get("error") or parsed.get("failure"))
            return True
        except Exception as e:
            log.exception("failed to set ECS task protection to %s: %s", enabled, e)
            return False

    def _should_renew_protection(self) -> bool:
        try:
            return int(self.redis_client.get(self._active_count_key) or 0) > 0
        except Exception:
            log.exception("failed to read active task count for %s", self.ecs_service)
            return False

    def _protection_renewal_loop(self) -> None:
        interval = max(60, self.protection_expires_minutes * 30)
        while True:
            time.sleep(interval)
            try:
                if self._should_renew_protection():
                    self._set_protection(True)
            except Exception:
                log.exception("protection renewal failed for %s", self.ecs_service)
