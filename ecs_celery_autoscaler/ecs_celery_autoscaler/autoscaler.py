from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

import boto3
from celery.app.control import flatten_reply
from celery.signals import after_task_publish, task_postrun, task_received
from celery.worker import state as worker_state
from celery.worker.control import inspect_command
from celery.worker.request import Request as WorkerRequest

log = logging.getLogger("ecs_celery_autoscaler")

LOCK_TIMEOUT = 30
LOCK_BLOCKING_TIMEOUT = 10
PROTECTION_POLL_INTERVAL = 5
INSPECT_TIMEOUT = 1.0
OUTSTANDING_COMMAND = "ecs_celery_autoscaler_outstanding"


def _matches_queue(request, queue_name) -> bool:
    return (request.delivery_info or {}).get("routing_key") == queue_name


def _count_matching_tasks(queue_name, consumer=None) -> int:
    """Counts reserved tasks (already including actively-executing ones, since Celery only removes
    them from reserved_requests on completion) plus eta/countdown tasks still waiting in
    `consumer`'s timer, filtered to `queue_name`. Shared by `_is_busy` and the OUTSTANDING_COMMAND
    control command so both use one race-free snapshot instead of three separate broadcasts a task
    could fall through the cracks of."""
    count = sum(1 for req in set(worker_state.reserved_requests) if _matches_queue(req, queue_name))
    if consumer is not None:
        for waiting in list(consumer.timer.schedule.queue):
            try:
                scheduled_request = waiting.entry.args[0]
            except (IndexError, TypeError):
                continue
            if isinstance(scheduled_request, WorkerRequest) and _matches_queue(scheduled_request, queue_name):
                count += 1
    return count


@inspect_command(name=OUTSTANDING_COMMAND, visible=False)
def _outstanding_for_queue(state, queue_name=None):
    """Registered as a Celery remote control command, so it always runs in the parent process
    (wherever Celery's pidbox listener lives) regardless of which process issues the broadcast."""
    return _count_matching_tasks(queue_name, state.consumer)


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
        self.agent_uri = os.environ.get("ECS_AGENT_URI")
        if not self.agent_uri:
            log.warning(
                "ECS_AGENT_URI is not set — %s will not use ECS task scale-in protection",
                ecs_service,
            )
        self.process_id = str(uuid.uuid4())
        self._ecs = ecs_client or boto3.client("ecs", region_name=aws_region)
        self._lock_key = f"ecs-celery-autoscaler:{self.ecs_service}:lock"
        self._consumer = None

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
        threading.Thread(target=self._protection_loop, daemon=True).start()
        log.info("Starting autoscaler with process_id: %s", self.process_id)

    def _on_publish(self, sender=None, routing_key=None, **kwargs):
        if routing_key == self.queue_name:
            self.scale_up()

    def _on_received(self, sender=None, request=None, **kwargs):
        """Fires the instant a task is delivered, before it necessarily has a pool slot or is even
        reserved. Protection must start here rather than at task_prerun, or a task still waiting
        for a slot could be stopped out from under it."""
        self._consumer = sender
        if request is not None and request.delivery_info.get("routing_key") == self.queue_name:
            self._set_protection(True)

    def _on_postrun(self, task_id=None, **kwargs):
        threading.Thread(target=self.maybe_scale_down, daemon=True).start()

    def scale_up(self) -> None:
        """Safe to call unconditionally and often — a no-op once desiredCount already meets target."""
        self._reconcile()

    def maybe_scale_down(self) -> None:
        """Recomputes the target worker count and scales toward it. Safe even mid-task, since ECS
        scale-in protection guarantees a busy container is never terminated regardless of
        desiredCount."""
        self._reconcile()

    def _reconcile(self) -> None:
        if not self.enabled:
            return
        try:
            with self.redis_client.lock(self._lock_key, timeout=LOCK_TIMEOUT, blocking_timeout=LOCK_BLOCKING_TIMEOUT):
                queue_len = self.redis_client.llen(self.queue_name)
                pending = self._pending_count()
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

    def _pending_count(self) -> int:
        """Cluster-wide count of tasks for this queue that are received but not finished, read live
        via Celery's control plane instead of a hand-maintained redis counter. Uses one
        OUTSTANDING_COMMAND broadcast rather than active()/reserved()/scheduled() separately, since a
        task could transition between those states in the gap between calls and be missed by all
        three. A worker that misses the timeout just isn't counted this pass and gets picked up on
        the next one."""
        replies = self.celery_app.control.broadcast(
            OUTSTANDING_COMMAND,
            arguments={"queue_name": self.queue_name},
            reply=True,
            timeout=INSPECT_TIMEOUT,
        )
        return sum(flatten_reply(replies or []).values())

    def _desired_count(self) -> int:
        resp = self._ecs.describe_services(cluster=self.ecs_cluster, services=[self.ecs_service])
        return resp["services"][0]["desiredCount"]

    def _set_protection(self, enabled: bool) -> bool:
        """Returns whether protection was confirmed set to `enabled`."""
        if not self.agent_uri:
            return True
        body: dict[str, Any] = {"ProtectionEnabled": enabled}
        if enabled:
            body["ExpiresInMinutes"] = self.protection_expires_minutes
        try:
            req = urllib.request.Request(
                f"{self.agent_uri}/task-protection/v1/state",
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

    def _is_busy(self) -> bool:
        """Whether this container has any task for this queue that's been received but not finished,
        read from Celery's own in-process state instead of a redis counter, since `reserved_requests`
        stays correct in the parent even under the prefork pool. Also checks the consumer's eta
        timer, since a scheduled task isn't reserved until it fires and would otherwise look idle.
        Assumes busy if the check itself fails, since this gates whether it's safe to renew or
        release protection and a failed check is not evidence of safety."""
        try:
            return _count_matching_tasks(self.queue_name, self._consumer) > 0
        except Exception:
            log.exception("failed to determine busy state for %s; assuming busy", self.ecs_service)
            return True

    def _protection_loop(self) -> None:
        """Renews protection every tick while busy; on the busy-to-idle transition, hands off to
        `_release_if_idle` rather than releasing directly, since a snapshot alone can't prove
        nothing arrives a moment later. Ticks on a fixed interval rather than reacting to events,
        since renewing a long-running task and retrying a failed initial engagement are its only
        real jobs. A few seconds of extra latency before a release attempt is immaterial next to
        `protection_expires_minutes`."""
        was_busy = False
        while True:
            time.sleep(PROTECTION_POLL_INTERVAL)
            try:
                busy = self._is_busy()
                if busy:
                    self._set_protection(True)
                elif was_busy:
                    self._release_if_idle()
                was_busy = busy
            except Exception:
                log.exception("protection poll failed for %s", self.ecs_service)

    def _release_if_idle(self) -> None:
        """Closes the check-then-act race on releasing protection: a task could arrive the instant
        after `_is_busy` says idle. Stops new deliveries first (`cancel_task_queue` via `call_soon`,
        since mutating the consumer off its own thread isn't safe), takes one final snapshot, and
        only releases if that's still empty, always resuming consumption afterward regardless of
        outcome since the pause is only to make the snapshot safe."""
        consumer = self._consumer
        if consumer is None:
            return

        def _pause_consumer_then_decide():
            try:
                consumer.cancel_task_queue(self.queue_name)
                if not self._is_busy():
                    self._set_protection(False)
            except Exception:
                log.exception("failed while releasing protection for %s", self.ecs_service)
            finally:
                consumer.add_task_queue(self.queue_name)

        consumer.call_soon(_pause_consumer_then_decide)
