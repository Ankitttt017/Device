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
  NEVER marks events SYNCED without future Main Server application ACK.
"""
from datetime import datetime, timedelta, timezone
import json
import logging
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

    def build_topic(self, machine_id: Optional[str] = None) -> str:
        """
        Builds the MQTT telemetry topic for the machine.
        """
        target_machine = machine_id or self.machine_id
        return build_telemetry_topic(self.mqtt_config.topic_prefix, target_machine)

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

        # 2. Query pending events in deterministic FIFO order
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
