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
from celery.signals import after_task_publish, task_postrun, task_received, worker_shutting_down
from celery.worker import state as worker_state
from celery.worker.control import inspect_command
from celery.worker.request import Request as WorkerRequest

log = logging.getLogger("ecs_celery_autoscaler")

LOCK_TIMEOUT = 90 # Set to be higher than boto's 60s timeout
LOCK_BLOCKING_TIMEOUT = 10
PROTECTION_POLL_INTERVAL = 5
RESUME_CHECK_RETRIES = 6
RESUME_CHECK_INTERVAL = 15
INSPECT_TIMEOUT = 1.0
OUTSTANDING_COMMAND = "ecs_celery_autoscaler_outstanding"


def _matches_queue(request, queue_name) -> bool:
    return (request.delivery_info or {}).get("routing_key") == queue_name


def _count_matching_tasks(queue_name, consumer=None) -> int:
    """Counts reserved tasks (already including actively-executing ones, since Celery only removes
    them from reserved_requests on completion) plus scheduled tasks still waiting in
    `consumer`'s timer, filtered to `queue_name`."""
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
        self._protection_lock = threading.Lock()
        self._shutting_down = threading.Event()
        self._was_busy = False

    def install(self) -> None:
        """Wire the signal handlers and start the protection renewal heartbeat."""
        if not self.enabled:
            log.warning("AUTOSCALING_ENABLED is false — %s autoscaler is disabled", self.ecs_service)
            return
        after_task_publish.connect(self._on_publish, weak=False)
        task_received.connect(self._on_received, weak=False)
        task_postrun.connect(self._on_postrun, weak=False)
        worker_shutting_down.connect(self._on_shutdown, weak=False)
        if not self.celery_app.conf.worker_disable_prefetch:
            log.warning(
                "worker_disable_prefetch is not enabled — workers that are already connected can "
                "prefetch tasks faster than they can run them, starving newly scaled-up workers of "
                "work. It is strongly recommended to enable this setting."
            )
        threading.Thread(target=self._protection_loop, daemon=True).start()
        log.info("Starting autoscaler with process_id: %s", self.process_id)

    def _on_publish(self, sender=None, routing_key=None, **kwargs):
        if routing_key == self.queue_name:
            threading.Thread(target=self.scale_up, daemon=True).start()

    def _on_received(self, sender=None, request=None, **kwargs):
        """Fires the instant a task is delivered, before it necessarily has a pool slot or is even
        reserved. Protection must start here rather than at task_prerun, or a task still waiting
        for a slot could be stopped out from under it."""
        self._consumer = sender
        if request is not None and request.delivery_info.get("routing_key") == self.queue_name:
            with self._protection_lock:
                self._set_protection(True)

    def _on_postrun(self, task_id=None, **kwargs):
        threading.Thread(target=self.maybe_scale_down, daemon=True).start()

    def _on_shutdown(self, sender=None, **kwargs):
        """Fires before the process exits, while the consumer is still pulling from the queue; sets
        `_shutting_down` and releases protection only if genuinely idle, since a busy container is
        already covered by ECS's stopTimeout. Unlike `_release_if_idle`, the queue isn't re-added
        afterward — shutdown is terminal."""
        self._shutting_down.set()
        consumer = self._consumer

        def _decide():
            try:
                self._release_protection_if_idle(consumer)
            except Exception:
                log.exception("failed while releasing protection on shutdown for %s", self.ecs_service)

        # A consumer that's still None never received a task, so there's no queue to cancel and no
        # thread-safety concern requiring call_soon.
        if consumer is None:
            _decide()
        else:
            consumer.call_soon(_decide)

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
        via Celery's control plane. A worker that misses the inspect timeout just isn't counted this
        pass and gets picked up on the next one."""
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
        except Exception:
            log.exception("failed to set ECS task protection to %s", enabled)
            return False
        reason = parsed.get("error") or parsed.get("failure")
        if reason:
            # Expected once ECS marks a task's convergence DEPLOYMENT_BLOCKED: it then refuses all
            # UpdateTaskProtection calls for that task, including our own renewal.
            log.warning("ECS declined to set task protection to %s: %s", enabled, reason)
            return False
        return True

    def _is_busy(self) -> bool:
        """Whether this container has any unfinished task for this queue, read from Celery's
        in-process state plus the consumer's eta timer (a scheduled task isn't reserved until it
        fires). Assumes busy if the check itself fails, for safety."""
        try:
            return _count_matching_tasks(self.queue_name, self._consumer) > 0
        except Exception:
            log.exception("failed to determine busy state for %s; assuming busy", self.ecs_service)
            return True

    def _protection_loop(self) -> None:
        """Sleeps between ticks and hands each tick to `_protection_tick`, scheduled via
        `consumer.call_soon` so it runs on the consumer's own thread instead of racing its mutation of
        `worker_state.reserved_requests` and the timer queue. Stops once `_shutting_down` is set."""
        while True:
            time.sleep(PROTECTION_POLL_INTERVAL)
            if self._shutting_down.is_set():
                return
            consumer = self._consumer
            if consumer is None:
                # No task has been received yet, so there's nothing consumer-owned to race against.
                self._protection_tick()
            else:
                consumer.call_soon(self._protection_tick)

    def _protection_tick(self) -> None:
        """Renews protection while busy, under `_protection_lock` so it can't race `_on_shutdown`'s
        release. On the busy-to-idle transition, hands off to `_release_if_idle` rather than releasing
        directly, since a snapshot alone can't prove nothing arrives a moment later."""
        try:
            busy = self._is_busy()
            if busy:
                with self._protection_lock:
                    if not self._shutting_down.is_set():
                        self._set_protection(True)
            elif self._was_busy:
                self._release_if_idle()
            self._was_busy = busy
        except Exception:
            log.exception("protection poll failed for %s", self.ecs_service)

    def _release_protection_if_idle(self, consumer) -> bool:
        """Cancels consumption of `queue_name`, then atomically rechecks busy state under
        `_protection_lock` and releases protection if idle, returning whether it did. Canceling first
        closes the gap where a task could arrive between check and release and run unprotected; call
        only from `consumer`'s own thread, since `cancel_task_queue` isn't thread-safe, and the lock
        also guards against racing the protection loop's renewal."""
        if consumer is not None:
            consumer.cancel_task_queue(self.queue_name)
        with self._protection_lock:
            if not self._is_busy():
                return self._set_protection(False)
        return False

    def _task_desired_status(self) -> str | None:
        """Reads this task's own DesiredStatus from the ECS task metadata endpoint. Returns None
        (inconclusive, not evidence either way) if the check can't be completed, including when no
        agent is configured."""
        if not self.agent_uri:
            return None
        try:
            with urllib.request.urlopen(f"{self.agent_uri}/task", timeout=5) as resp:
                parsed = json.loads(resp.read())
        except Exception:
            log.exception("failed to check task status for %s", self.ecs_service)
            return None
        return parsed.get("DesiredStatus")

    def _resume_after_confirming(self, consumer, attempts_left: int) -> None:
        """Polls this task's DesiredStatus every `RESUME_CHECK_INTERVAL`, up to `RESUME_CHECK_RETRIES`
        times: STOPPED stops retries for good since `_on_shutdown` takes over, while anything else
        (RUNNING, or a failed check) keeps retrying and resumes unconditionally once retries run out,
        since a permanently orphaned consumer is worse than reopening the race this closes.
        `_shutting_down` is checked once, right after that blocking call, since resuming after
        shutdown starts would defeat its terminal queue-pause -- `_task_desired_status` returns None
        with no network call when there's no agent, so that case flows through the same check for
        free."""
        status = self._task_desired_status()
        if self._shutting_down.is_set():
            return
        if status == "STOPPED":
            return
        if self.agent_uri and attempts_left > 1:
            consumer.timer.call_after(
                RESUME_CHECK_INTERVAL, self._resume_after_confirming, (consumer, attempts_left - 1)
            )
        else:
            consumer.add_task_queue(self.queue_name)

    def _release_if_idle(self) -> None:
        """Removes protection on a worker if it is not currently busy with any jobs."""
        consumer = self._consumer
        if consumer is None:
            return

        def _pause_consumer_then_decide():
            released = False
            try:
                released = self._release_protection_if_idle(consumer)
            except Exception:
                log.exception("failed while releasing protection for %s", self.ecs_service)
            finally:
                # We need to add the consumer back even if we released protection on this worker. This is because
                # the ECS agent may choose to kill a different worker with no active protection. If that happens, this
                # worker needs to be ready to receive future jobs. If we did release protection, don't resume until
                # `_resume_after_confirming` has confirmed ECS hasn't already decided to stop this task.
                if released:
                    consumer.timer.call_after(
                        RESUME_CHECK_INTERVAL, self._resume_after_confirming, (consumer, RESUME_CHECK_RETRIES)
                    )
                else:
                    consumer.add_task_queue(self.queue_name)
        # Use call_soon to ensure thread safety
        consumer.call_soon(_pause_consumer_then_decide)
