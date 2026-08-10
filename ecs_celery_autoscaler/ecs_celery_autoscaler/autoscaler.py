from __future__ import annotations

import abc
import json
import logging
import math
import os
import threading
import time
import urllib.request
import uuid
from typing import Any

import boto3
from celery.app.control import flatten_reply
from celery.signals import (
    after_task_publish,
    before_task_publish,
    task_postrun,
    task_received,
    worker_shutting_down,
)
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
PUBLISHED_AT_HEADER = "ecs_celery_autoscaler_published_at"


def _matches_queue(request, queue_name) -> bool:
    return (request.delivery_info or {}).get("routing_key") == queue_name


def _count_matching_tasks(queue_name, consumer=None) -> int:
    """Counts reserved tasks for `queue_name` (includes actively-executing ones, since Celery only
    clears them on completion) plus tasks still waiting in `consumer`'s timer."""
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
    regardless of which process issues the broadcast."""
    return _count_matching_tasks(queue_name, state.consumer)


class ScalingMetric(abc.ABC):
    """Pluggable scaling signal for `EcsCeleryAutoscaler`; exactly one metric drives a given instance."""

    def bind(self, autoscaler: EcsCeleryAutoscaler) -> None:
        """Called once from `install()`; override to wire extra signal handlers, calling `super().bind(...)` first."""
        self.autoscaler = autoscaler
        self.redis_client = autoscaler.redis_client

    @abc.abstractmethod
    def target_worker_count(self, *, current: int, pending: int) -> tuple[int, dict]:
        """Returns (raw target worker count, extra fields for the scale-event log), before the caller
        applies the busy-worker floor and min/max clamp."""

    def on_task_received(self, request) -> None:
        """Optional hook fired from `_on_received` after scale-in protection is set; no-op by default."""


class QueueDepthMetric(ScalingMetric):
    """Scales on Celery queue depth: ceil((broker queue length + in-flight tasks) / tasks_per_worker)."""

    def __init__(self, tasks_per_worker: int = 1):
        self.tasks_per_worker = tasks_per_worker

    def target_worker_count(self, *, current: int, pending: int) -> tuple[int, dict]:
        queue_len = self.redis_client.llen(self.autoscaler.queue_name)
        outstanding = queue_len + pending
        return math.ceil(outstanding / self.tasks_per_worker), {"queue_len": queue_len}


class QueueLatencyMetric(ScalingMetric):
    """Scales by ratcheting the current worker count based on how long tasks wait in the queue before
    being received, measured by diffing a publish-time timestamp stamped into the task headers. Every
    process that calls `install()` must use this same metric, including producers, or no timestamps
    get stamped and this metric silently holds `current` forever."""

    def __init__(
        self,
        threshold_seconds: float,
        window_seconds: int = 300,
        scale_in_cooldown_seconds: int = 300,
    ):
        self.threshold_seconds = threshold_seconds
        self.window_seconds = window_seconds
        self.scale_in_cooldown_seconds = scale_in_cooldown_seconds

    def bind(self, autoscaler: EcsCeleryAutoscaler) -> None:
        super().bind(autoscaler)
        self._latency_key = f"ecs-celery-autoscaler:{autoscaler.ecs_service}:queue-latency-samples"
        self._cooldown_key = f"ecs-celery-autoscaler:{autoscaler.ecs_service}:queue-latency-scale-in-cooldown"
        before_task_publish.connect(self._on_before_publish, weak=False)

    def _on_before_publish(self, sender=None, headers=None, routing_key=None, **kwargs):
        if routing_key == self.autoscaler.queue_name and headers is not None:
            headers[PUBLISHED_AT_HEADER] = time.time()

    def on_task_received(self, request) -> None:
        """Records this task's queue wait as a sample, skipping delayed tasks (eta/countdown/backoff
        retries) since their wait is intentional, not backlog."""
        if request.eta is not None:
            return
        published_at = request.message.headers.get(PUBLISHED_AT_HEADER)
        if published_at is None:
            return
        try:
            wait = max(0.0, time.time() - float(published_at))
            member = f"{wait:.4f}:{uuid.uuid4().hex[:8]}"
            with self.redis_client.pipeline() as pipe:
                pipe.zadd(self._latency_key, {member: time.time()})
                pipe.expire(self._latency_key, self.window_seconds * 2)
                pipe.execute()
        except Exception:
            log.exception("failed to record queue latency sample for %s", self.autoscaler.ecs_service)

    def _current_latency(self) -> float | None:
        """Returns the max sample recorded within `window_seconds`, pruning older ones first."""
        redis_client = self.redis_client
        redis_client.zremrangebyscore(self._latency_key, "-inf", time.time() - self.window_seconds)
        members = redis_client.zrange(self._latency_key, 0, -1)
        if not members:
            return None
        return max(float((m.decode() if isinstance(m, bytes) else m).split(":", 1)[0]) for m in members)

    def _scale_in_allowed(self) -> bool:
        """Atomic cluster-wide cooldown gate: only the first caller within a window may scale in."""
        return bool(
            self.redis_client.set(self._cooldown_key, "1", ex=self.scale_in_cooldown_seconds, nx=True)
        )

    def target_worker_count(self, *, current: int, pending: int) -> tuple[int, dict]:
        latency = self._current_latency()
        log_extra = {"queue_latency": f"{latency:.2f}s" if latency is not None else "n/a"}
        if latency is None:
            return current, log_extra
        if latency > self.threshold_seconds:
            return current + 1, log_extra
        if self._scale_in_allowed():
            return current - 1, log_extra
        return current, log_extra


class EcsCeleryAutoscaler:
    """Scales one ECS service's desiredCount across min_workers-max_workers using a pluggable
    `ScalingMetric`. Safety against killing a busy worker is delegated to ECS task scale-in
    protection rather than handled by this class."""

    def __init__(
        self,
        *,
        celery_app,
        redis_client,
        ecs_cluster: str,
        ecs_service: str,
        aws_region: str,
        metric: ScalingMetric,
        queue_name: str = "celery",
        min_workers: int = 0,
        max_workers: int = 1,
        protection_expires_minutes: int = 60,
        ecs_client: Any = None,
    ):
        self.celery_app = celery_app
        self.redis_client = redis_client
        self.ecs_cluster = ecs_cluster
        self.ecs_service = ecs_service
        self.metric = metric
        self.queue_name = queue_name
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.protection_expires_minutes = protection_expires_minutes
        self.enabled = os.environ.get("AUTOSCALING_ENABLED", "True") not in ("FALSE", "False", "false")
        self.agent_uri = os.environ.get("ECS_AGENT_URI")
        self.metadata_uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
        self.process_id = str(uuid.uuid4())
        self._ecs = ecs_client or boto3.client("ecs", region_name=aws_region)
        self._lock_key = f"ecs-celery-autoscaler:{self.ecs_service}:lock"
        self._consumer = None
        self._protection_lock = threading.Lock()
        self._shutting_down = threading.Event()
        self._was_busy = False

    def install(self) -> None:
        """Wire the signal handlers and start the protection renewal heartbeat."""
        # Check configuration first and error/warn depending on severity
        if not self.enabled:
            log.warning("AUTOSCALING_ENABLED is false — %s will not use ECSCeleryAutoscaler", self.ecs_service)
            return
        if not self.agent_uri:
            log.error(
                "ECS_AGENT_URI is not set — %s will not use ECSCeleryAutoscaler",
                self.ecs_service,
            )
            return
        if not self.metadata_uri:
            log.warning(
                "ECS_CONTAINER_METADATA_URI_V4 is not set — %s will not use ECSCeleryAutoscaler",
                self.ecs_service,
            )
            return

        if not self.celery_app.conf.worker_disable_prefetch:
            log.warning(
                "worker_disable_prefetch is not enabled — workers that are already connected can "
                "prefetch tasks faster than they can run them, starving newly scaled-up workers of "
                "work. It is strongly recommended to enable this setting."
            )

        self.metric.bind(self)

        # Wire signals
        after_task_publish.connect(self._on_publish, weak=False)
        task_received.connect(self._on_received, weak=False)
        task_postrun.connect(self._on_postrun, weak=False)
        worker_shutting_down.connect(self._on_shutdown, weak=False)

        # Start background thread responsible for renewing task protection
        threading.Thread(target=self._protection_loop, daemon=True).start()

        log.info("Starting autoscaler with process_id: %s", self.process_id)

    def _on_publish(self, sender=None, routing_key=None, **kwargs):
        if routing_key == self.queue_name:
            threading.Thread(target=self.maybe_scale_service, daemon=True).start()

    def _on_received(self, sender=None, request=None, **kwargs):
        """Fires the instant a task is delivered, before it's necessarily reserved or has a pool
        slot. Protection must start here rather than at task_prerun, or a task still waiting for
        a slot could be stopped out from under it."""
        self._consumer = sender
        if request is not None and request.delivery_info.get("routing_key") == self.queue_name:
            with self._protection_lock:
                self._set_protection(True)
            self.metric.on_task_received(request)

    def _on_postrun(self, task_id=None, **kwargs):
        threading.Thread(target=self.maybe_scale_service, daemon=True).start()

    def _on_shutdown(self, sender=None, **kwargs):
        """Fires before the process exits; releases protection only if genuinely idle, since a busy
        container is already covered by ECS's stopTimeout. Unlike `_release_protection_resumable`,
        the queue isn't re-added afterward — shutdown is terminal."""
        self._shutting_down.set()
        consumer = self._consumer

        def _release_protection_wrapper():
            try:
                self._release_protection_terminal(consumer)
            except Exception:
                log.exception("failed while releasing protection on shutdown for %s", self.ecs_service)

        # A consumer that's still None never received a task, so there's no queue to cancel and no
        # thread-safety concern requiring call_soon.
        if consumer is None:
            _release_protection_wrapper()
        else:
            consumer.call_soon(_release_protection_wrapper)

    def maybe_scale_service(self) -> None:
        """Recomputes the target worker count and scales toward it. Safe even mid-task, since ECS
        scale-in protection guarantees a busy container is never terminated regardless of
        desiredCount."""
        if not self.enabled:
            return
        try:
            with self.redis_client.lock(self._lock_key, timeout=LOCK_TIMEOUT, blocking_timeout=LOCK_BLOCKING_TIMEOUT):
                current = self._desired_count()
                pending, busy_workers = self._pending_and_busy_workers()
                raw_target, log_extra = self.metric.target_worker_count(current=current, pending=pending)
                # Never target fewer workers than are currently busy: a busy worker's task can't be
                # moved to another container, so asking ECS to scale below that just gets the
                # deployment blocked by scale-in protection until the task finishes on its own.
                target = max(busy_workers, min(self.max_workers, max(self.min_workers, raw_target)))
                if target != current:
                    self._ecs.update_service(cluster=self.ecs_cluster, service=self.ecs_service, desiredCount=target)
                    log.info(
                        "scaled %s to desiredCount=%d (pending=%d, busy_workers=%d, %s)",
                        self.ecs_service,
                        target,
                        pending,
                        busy_workers,
                        ", ".join(f"{k}={v}" for k, v in log_extra.items()),
                    )
        except Exception:
            log.exception("Autoscaling failed for %s", self.ecs_service)

    def _pending_and_busy_workers(self) -> tuple[int, int]:
        """Cluster-wide count of tasks for this queue that are received but not finished, plus how
        many distinct workers reported at least one, read live via Celery's control plane. A worker
        that misses the inspect timeout just isn't counted this pass and gets picked up on the next
        one."""
        replies = self.celery_app.control.broadcast(
            OUTSTANDING_COMMAND,
            arguments={"queue_name": self.queue_name},
            reply=True,
            timeout=INSPECT_TIMEOUT,
        )
        counts = flatten_reply(replies or []).values()
        return sum(counts), sum(1 for count in counts if count > 0)

    def _desired_count(self) -> int:
        resp = self._ecs.describe_services(cluster=self.ecs_cluster, services=[self.ecs_service])
        return resp["services"][0]["desiredCount"]

    def _set_protection(self, enabled: bool) -> bool:
        """Returns whether protection was confirmed set to `enabled`."""
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
        """Ticks every `PROTECTION_POLL_INTERVAL` seconds via `consumer.call_soon`, keeping
        `_protection_tick` on the consumer's own thread instead of racing Celery's state. Never stops,
        even after shutdown, since a task busy at that point may not go idle until long after."""
        while True:
            time.sleep(PROTECTION_POLL_INTERVAL)
            consumer = self._consumer
            try:
                if consumer is None:
                    # No task has been received yet, so there's nothing consumer-owned to race against.
                    self._protection_tick()
                else:
                    consumer.call_soon(self._protection_tick)
            except Exception:
                log.exception("failed to schedule protection tick for %s", self.ecs_service)

    def _protection_tick(self) -> None:
        """Renews protection while busy, under `_protection_lock` so it can't race `_on_shutdown`'s
        release. On the busy-to-idle transition, releases terminally if shutdown was already signaled
        (since `_on_shutdown`'s one-shot check can miss a later transition); otherwise defers to
        `_release_protection_resumable`."""
        try:
            busy = self._is_busy()
            shutting_down = self._shutting_down.is_set()
            if busy:
                with self._protection_lock:
                    if not shutting_down:
                        self._set_protection(True)
            elif self._was_busy:
                if shutting_down:
                    self._release_protection_terminal(self._consumer)
                else:
                    self._release_protection_resumable()
            self._was_busy = busy
        except Exception:
            log.exception("protection poll failed for %s", self.ecs_service)

    def _task_desired_status(self) -> str | None:
        """Reads this task's own DesiredStatus from the ECS task metadata endpoint. Returns None
        (inconclusive either way) if the check can't be completed."""
        try:
            with urllib.request.urlopen(f"{self.metadata_uri}/task", timeout=5) as resp:
                parsed = json.loads(resp.read())
        except Exception:
            log.exception("failed to check task status for %s", self.ecs_service)
            return None
        return parsed.get("DesiredStatus")

    def _resume_after_confirming(self, consumer, attempts_left: int) -> None:
        """Runs on the consumer's own thread, but offloads the DesiredStatus check to a throwaway
        thread so it doesn't stall the event loop. The result is handed back via `call_soon`,
        keeping queue/timer mutation on the consumer thread."""
        threading.Thread(target=self._check_status_then_decide, args=(consumer, attempts_left), daemon=True).start()

    def _check_status_then_decide(self, consumer, attempts_left: int) -> None:
        try:
            status = self._task_desired_status()
            consumer.call_soon(self._decide_resume, consumer, attempts_left, status)
        except Exception:
            log.exception("failed to schedule resume decision for %s", self.ecs_service)

    def _decide_resume(self, consumer, attempts_left: int, status: str | None) -> None:
        """The ECS agent decides when to mark this task STOPPED on no fixed schedule, so check a few
        times over RESUME_CHECK_INTERVAL to give it time. If the task is still RUNNING once attempts
        run out, assume ECS decided to keep it and re-connect the queue so it can receive jobs again."""
        if self._shutting_down.is_set():
            return
        if status == "STOPPED":
            return
        if attempts_left > 1:
            consumer.timer.call_after(
                RESUME_CHECK_INTERVAL, self._resume_after_confirming, (consumer, attempts_left - 1)
            )
        else:
            consumer.add_task_queue(self.queue_name)

    def _release_protection_resumable(self) -> None:
        """Removes protection on a worker if it is not currently busy with any jobs."""
        consumer = self._consumer
        if consumer is None:
            return

        def _pause_consumer_then_decide():
            released = False
            try:
                released = self._release_protection_terminal(consumer)
            except Exception:
                log.exception("failed while releasing protection for %s", self.ecs_service)
            finally:
                # Add the consumer back even if we released protection on this worker. This is because
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

    def _release_protection_terminal(self, consumer) -> bool:
        """Cancels `queue_name` consumption, then atomically rechecks busy state under
        `_protection_lock` and releases protection if idle, returning whether it did. Call only from
        `consumer`'s own thread — canceling first closes the arrival gap between check and release,
        and `cancel_task_queue` isn't thread-safe."""
        if consumer is not None:
            consumer.cancel_task_queue(self.queue_name)
        with self._protection_lock:
            if not self._is_busy():
                return self._set_protection(False)
        return False