"""
Synchronization Manager Module for QUAD Gateway.
Phase 3: Reliably drains PENDING SQLite acquisition events and publishes them to the MQTT broker.

Guarantees:
- Reads oldest pending events first (FIFO order).
- Enforces bounded batch size (prevents unbounded memory consumption).
- Non-blocking execution (does not block live PLC data acquisition).
- Resilient exponential backoff when broker is unreachable or drops messages.
- Zero data loss: events remain safely in SQLite when broker is offline.
- Idempotency preservation: preserves immutable acquisition event_uuid.
- Strict Phase-3 safety: transitions events to SENDING upon publish acceptance.
  Events become SYNCED only when the Main Server ACKs them on <prefix>/<machine_id>/ack.
- SENDING events without an ACK within ack_timeout_seconds are published again.

Server ACK message (JSON), status SAVED or DUPLICATE -> SYNCED, ERROR -> retried:
    {"event_uuid": "<uuid>", "status": "SAVED"}
    {"event_uuids": ["<uuid>", ...], "status": "SAVED"}
"""
from datetime import datetime, timedelta, timezone
import json
import logging
import queue
from typing import Any, Dict, List, Optional

from quad_gateway.config.config_loader import MQTTConfig
from quad_gateway.storage.database import StorageManager, utc_now_str

logger = logging.getLogger("quad_gateway.sync")


def build_telemetry_topic(topic_prefix: str, machine_id: str) -> str:
    """
    Constructs systematic telemetry topic:
    <topic_prefix>/<machine_id>/telemetry
    Example: quad/UBE-850T-02/telemetry
    """
    clean_prefix = topic_prefix.strip("/")
    return f"{clean_prefix}/{machine_id}/telemetry"


def build_ack_topic(topic_prefix: str, machine_id: str) -> str:
    """
    Topic on which the Main Server confirms stored events:
    <topic_prefix>/<machine_id>/ack  (e.g. quad/UBE-850T-02/ack)
    """
    return f"{topic_prefix.strip('/')}/{machine_id}/ack"


def build_status_topic(topic_prefix: str, machine_id: str) -> str:
    """
    Retained QUAD online/offline status topic: <topic_prefix>/<machine_id>/status
    """
    return f"{topic_prefix.strip('/')}/{machine_id}/status"


SHOT_RESULT_BY_STATUS = {1: "OK", 3: "WARM_UP", 5: "NG"}


def build_shot_identity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Summarizes the shot so the server can match it with its existing records:
    machine shot number, PLC shot date/time (D2100-D2105), result and part name.
    Missing or unreadable values are None (never guessed).
    """
    def val(tag: str) -> Any:
        point = payload.get(tag) or {}
        return point.get("value") if point.get("quality", "GOOD") == "GOOD" else None

    parts = [val(t) for t in ("SHOT_YEAR", "SHOT_MONTH", "SHOT_DAY", "SHOT_HOUR", "SHOT_MINUTE", "SHOT_SECOND")]
    shot_time = None
    if all(isinstance(p, int) for p in parts):
        year = parts[0] + 2000 if parts[0] < 100 else parts[0]
        try:
            shot_time = datetime(year, *parts[1:]).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            shot_time = None

    result = val("SHOT_RESULT")
    if result is None:
        result = SHOT_RESULT_BY_STATUS.get(val("SHOT_STATUS"))

    return {
        "shot_no": val("SHOT_NO"),
        "shot_time": shot_time,
        "result": result,
        "part_name": val("PART_NAME"),
    }


class SyncManager:
    """
    Orchestrates extraction of PENDING telemetry events from SQLite and
    publishes them to the configured MQTT broker.
    """

    def __init__(
        self,
        storage: StorageManager,
        publisher: Any,
        mqtt_config: MQTTConfig,
        machine_id: str,
    ):
        self.storage = storage
        self.publisher = publisher
        self.mqtt_config = mqtt_config
        self.machine_id = machine_id

        # ACKs arrive on the MQTT network thread; they are queued and applied to
        # SQLite from the main loop in process_acks().
        self._acks: "queue.Queue[tuple]" = queue.Queue()
        self.ack_topic = build_ack_topic(mqtt_config.topic_prefix, machine_id)
        if hasattr(publisher, "subscribe"):
            publisher.subscribe(self.ack_topic, self._on_ack_message)

    def build_topic(self, machine_id: Optional[str] = None) -> str:
        """
        Builds the MQTT telemetry topic for the machine.
        """
        target_machine = machine_id or self.machine_id
        return build_telemetry_topic(self.mqtt_config.topic_prefix, target_machine)

    def _on_ack_message(self, topic: str, payload: str) -> None:
        """
        Parses a server ACK (runs on the MQTT thread; only queues, never touches SQLite).
        """
        try:
            msg = json.loads(payload)
            uuids = msg.get("event_uuids") or ([msg["event_uuid"]] if msg.get("event_uuid") else [])
            status = str(msg.get("status", "SAVED")).upper()
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logger.warning(f"Ignoring malformed ACK on {topic}: {e}")
            return
        if uuids:
            self._acks.put((status, [str(u) for u in uuids], msg.get("error")))

    def process_acks(self) -> Dict[str, int]:
        """
        Applies all queued server ACKs: SAVED/DUPLICATE -> SYNCED, ERROR -> retry later.
        """
        synced, failed = [], []
        errors = []
        while True:
            try:
                status, uuids, error = self._acks.get_nowait()
            except queue.Empty:
                break
            if status in ("SAVED", "DUPLICATE"):
                synced.extend(uuids)
            else:
                failed.extend(uuids)
                errors.append(error or status)

        result = {"synced": 0, "rejected": 0}
        if synced:
            result["synced"] = self.storage.mark_events_synced(synced)
        if failed:
            retry_at = (datetime.now(timezone.utc) + timedelta(seconds=self.mqtt_config.retry_max_delay)).isoformat()
            result["rejected"] = self.storage.mark_events_failed(
                failed, f"Server error: {'; '.join(map(str, errors))[:500]}", retry_at
            )
            logger.warning(f"Server reported an error for {len(failed)} event(s); they will be resent.")
        return result

    def requeue_unacknowledged(self) -> int:
        """
        Re-queues events published more than ack_timeout_seconds ago that the server never ACKed.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.mqtt_config.ack_timeout_seconds)
        count = self.storage.requeue_unacknowledged(cutoff.isoformat())
        if count:
            logger.warning(f"{count} event(s) got no server ACK within {self.mqtt_config.ack_timeout_seconds:.0f}s; resending.")
        return count

    def run_cycle(self, batch_size: Optional[int] = None) -> Dict[str, Any]:
        """
        One sync step for the main loop: apply ACKs, re-queue unACKed events, publish one batch.
        """
        acks = self.process_acks()
        requeued = self.requeue_unacknowledged()
        result = self.sync_pending_events(batch_size=batch_size)
        result.update(acked=acks["synced"], rejected=acks["rejected"], requeued=requeued)
        return result

    def purge_synced(self, retention_days: int) -> int:
        """
        Deletes local events the server confirmed more than retention_days ago.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        count = self.storage.purge_synced_events(cutoff.isoformat())
        if count:
            logger.info(f"Retention: removed {count} server-confirmed event(s) older than {retention_days} days.")
        return count

    def calculate_backoff_delay(self, attempt_count: int) -> float:
        """
        Calculates exponential backoff delay capped at retry_max_delay.
        Delay = min(initial * (factor ^ (attempt - 1)), max_delay)
        """
        exponent = max(0, attempt_count - 1)
        delay = self.mqtt_config.retry_initial_delay * (
            self.mqtt_config.retry_backoff_factor ** exponent
        )
        return min(delay, self.mqtt_config.retry_max_delay)

    def sync_pending_events(
        self, batch_size: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Processes up to batch_size pending/failed events whose retry time has elapsed.
        Publishes to MQTT and updates sync_queue state accordingly.

        Returns:
            Dict summarizing: status, attempted, published, failed, skipped
        """
        limit = batch_size or self.mqtt_config.batch_size

        # 1. Offline safety check: if publisher is not connected, do not attempt publish
        if not getattr(self.publisher, "is_connected", False):
            logger.debug(
                "MQTT publisher is currently offline. Pending events remain safely buffered in SQLite."
            )
            return {
                "status": "OFFLINE",
                "attempted": 0,
                "published": 0,
                "failed": 0,
                "skipped": 0,
            }

        # 2. Flow control: never have more than max_unacked events waiting for a server ACK,
        #    so a stopped server subscriber cannot cause a flood of resends.
        awaiting_ack = self.storage.get_sync_status_counts().get("SENDING", 0)
        limit = min(limit, self.mqtt_config.max_unacked - awaiting_ack)
        if limit <= 0:
            logger.debug(f"{awaiting_ack} event(s) awaiting server ACK (limit {self.mqtt_config.max_unacked}); pausing publish.")
            return {
                "status": "WAITING_ACK",
                "attempted": 0,
                "published": 0,
                "failed": 0,
                "skipped": 0,
            }

        # 3. Query pending events in deterministic FIFO order
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()
        records = self.storage.get_pending_sync_events(limit=limit, now_iso=now_iso)

        if not records:
            return {
                "status": "IDLE",
                "attempted": 0,
                "published": 0,
                "failed": 0,
                "skipped": 0,
            }

        published_count = 0
        failed_count = 0

        # 3. Process each event sequentially
        for rec in records:
            event_uuid = rec["event_uuid"]
            machine_id = rec["machine_id"]
            topic = self.build_topic(machine_id)
            current_attempts = rec.get("attempt_count", 0)

            # Build standardized MQTT telemetry payload preserving all attributes
            message_obj = {
                "event_uuid": event_uuid,
                "gateway_id": rec["gateway_id"],
                "machine_id": machine_id,
                "acquired_at": rec["acquired_at"],
                "quality": rec["quality"],
                "shot": build_shot_identity(rec["payload_decoded"]),
                "payload": rec["payload_decoded"],
            }
            payload_str = json.dumps(message_obj, ensure_ascii=False)

            # Attempt publish via publisher
            publish_ok = self.publisher.publish(
                topic=topic,
                payload=payload_str,
                qos=self.mqtt_config.qos,
                retain=self.mqtt_config.retain,
            )

            if publish_ok:
                # Event accepted by MQTT client -> transition to SENDING (awaiting future server ACK)
                new_attempts = current_attempts + 1
                self.storage.update_sync_attempt(
                    event_uuid=event_uuid,
                    status="SENDING",
                    attempt_count=new_attempts,
                    last_error=None,
                    next_attempt_at=None,
                    synced_at=None,  # NEVER mark SYNCED without future Main Server application ACK
                )
                published_count += 1
                logger.info(
                    f"Published event {event_uuid} to {topic} (QoS {self.mqtt_config.qos}, status -> SENDING)."
                )
            else:
                # Publish failed or rejected -> calculate backoff schedule and record failure
                new_attempts = current_attempts + 1
                delay_sec = self.calculate_backoff_delay(new_attempts)
                next_retry_iso = (now_dt + timedelta(seconds=delay_sec)).isoformat()

                self.storage.update_sync_attempt(
                    event_uuid=event_uuid,
                    status="FAILED",
                    attempt_count=new_attempts,
                    last_error="MQTT publish failed or network buffer rejected message",
                    next_attempt_at=next_retry_iso,
                    synced_at=None,
                )
                failed_count += 1
                logger.warning(
                    f"Failed to publish event {event_uuid} to {topic}. "
                    f"Scheduled retry attempt #{new_attempts + 1} in {delay_sec:.1f}s (at {next_retry_iso})."
                )

        return {
            "status": "SUCCESS" if published_count > 0 else "PARTIAL",
            "attempted": len(records),
            "published": published_count,
            "failed": failed_count,
            "skipped": 0,
        }
