"""
MQTT Publisher Module for QUAD Gateway.
Phase 3: Persistent, resilient MQTT publishing for edge telemetry events.

Guarantees:
- Persistent connection (connect once, maintain connection across cycles).
- Automatic reconnection with controlled backoff.
- Zero password leakage (credentials never printed in logs).
- Clean separation from SQLite storage and SLMP acquisition.
- Clean shutdown on gateway termination.
- Zero data loss: offline events stay buffered in SQLite.
"""
import json
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("quad_gateway.mqtt")


class MQTTPublisher:
    """
    Industrial MQTT Publisher for QUAD Gateway using paho-mqtt.
    Maintains a persistent connection to the broker and publishes serialized events.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 1883,
        client_id: str = "QUAD-01",
        username: Optional[str] = None,
        password: Optional[str] = None,
        keepalive: int = 60,
        qos: int = 1,
        retain: bool = False,
    ):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.username = username
        self.password = password
        self.keepalive = keepalive
        self.qos = qos
        self.retain = retain

        self._connected = False
        self._client: Optional[Any] = None
        self._loop_running = False

    @property
    def is_connected(self) -> bool:
        return self._connected and self._client is not None

    def start(self) -> bool:
        """
        Initializes paho client, configures credentials, attaches callbacks,
        and starts background network thread.
        Fails safely without crashing if broker is unreachable.
        """
        if self.is_connected:
            return True

        try:
            import paho.mqtt.client as mqtt

            # Use paho 2.x API if available
            if hasattr(mqtt, "CallbackAPIVersion"):
                self._client = mqtt.Client(
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                    client_id=self.client_id,
                    clean_session=True,
                )
            else:
                self._client = mqtt.Client(
                    client_id=self.client_id,
                    clean_session=True,
                )

            # Configure credentials securely (never log password)
            if self.username:
                self._client.username_pw_set(self.username, self.password)
                logger.info(f"Configured MQTT authentication for user '{self.username}' (password redacted).")

            # Attach event callbacks
            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.on_publish = self._on_publish

            logger.info(f"Connecting to MQTT broker at {self.host}:{self.port} (Client ID: {self.client_id}, Keepalive: {self.keepalive}s)...")
            # Connect async to avoid blocking acquisition if broker is temporarily down
            self._client.connect_async(self.host, self.port, keepalive=self.keepalive)
            self._client.loop_start()
            self._loop_running = True
            return True

        except Exception as e:
            logger.warning(
                f"Could not connect to MQTT broker at {self.host}:{self.port}: {e}. "
                "Gateway will buffer all telemetry locally in SQLite until broker is reachable."
            )
            self._connected = False
            return False

    def stop(self) -> None:
        """
        Stops background loop and disconnects cleanly.
        """
        logger.info("Stopping MQTT publisher...")
        if self._client:
            try:
                if self._loop_running:
                    self._client.loop_stop()
                    self._loop_running = False
                self._client.disconnect()
            except Exception as e:
                logger.debug(f"Error during MQTT client disconnect: {e}")
        self._connected = False
        self._client = None
        logger.info("MQTT publisher stopped cleanly.")

    def publish(
        self,
        topic: str,
        payload: str,
        qos: Optional[int] = None,
        retain: Optional[bool] = None,
    ) -> bool:
        """
        Publishes serialized event payload to topic.
        Returns True if message was successfully queued to network buffer, False otherwise.
        """
        if not self.is_connected or not self._client:
            logger.debug(f"Cannot publish to {topic}: MQTT client is not connected.")
            return False

        use_qos = qos if qos is not None else self.qos
        use_retain = retain if retain is not None else self.retain

        try:
            import paho.mqtt.client as mqtt

            msg_info = self._client.publish(
                topic=topic,
                payload=payload,
                qos=use_qos,
                retain=use_retain,
            )
            if msg_info.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.debug(f"Message queued for topic {topic} (mid={msg_info.mid}, QoS={use_qos}).")
                return True
            else:
                logger.warning(f"Publish to {topic} rejected with MQTT code {msg_info.rc}.")
                return False

        except Exception as e:
            logger.warning(f"Exception publishing to {topic}: {e}")
            return False

    # Callbacks
    def _on_connect(self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any = None) -> None:
        # Handles both paho 1.x (rc int) and 2.x (ReasonCode)
        is_success = False
        rc_val = getattr(reason_code, "value", reason_code)
        if rc_val == 0:
            is_success = True

        if is_success:
            self._connected = True
            logger.info(f"[MQTT CONNECTED] Successfully connected to broker at {self.host}:{self.port} (Client: {self.client_id}).")
        else:
            self._connected = False
            logger.warning(f"[MQTT CONNECT REJECTED] Broker rejected connection: code={reason_code}.")

    def _on_disconnect(self, client: Any, userdata: Any, disconnect_flags_or_rc: Any, reason_code: Any = None, properties: Any = None) -> None:
        self._connected = False
        rc = reason_code if reason_code is not None else disconnect_flags_or_rc
        logger.warning(f"[MQTT DISCONNECTED] Connection to broker at {self.host}:{self.port} lost (reason={rc}). Will reconnect automatically.")

    def _on_publish(self, client: Any, userdata: Any, mid: int, reason_code: Any = None, properties: Any = None) -> None:
        logger.debug(f"MQTT puback received for mid={mid}.")


class FakeMQTTPublisher:
    """
    In-memory mock MQTT publisher for unit, integration, and offline testing.
    Records published messages and simulates connection state / failure modes.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 1883,
        client_id: str = "FAKE-QUAD",
        **kwargs,
    ):
        self.host = host
        self.port = port
        self._is_connected = True
        self.offline_mode = False
        self.fail_publish = False
        self.published_messages: List[Dict[str, Any]] = []

    @property
    def is_connected(self) -> bool:
        return self._is_connected and not self.offline_mode

    def start(self) -> bool:
        self._is_connected = not self.offline_mode
        return self._is_connected

    def stop(self) -> None:
        self._is_connected = False

    def publish(
        self,
        topic: str,
        payload: str,
        qos: int = 1,
        retain: bool = False,
    ) -> bool:
        if not self.is_connected or self.fail_publish:
            return False

        msg = {
            "topic": topic,
            "payload": payload,
            "payload_dict": json.loads(payload),
            "qos": qos,
            "retain": retain,
            "published_at": time.time(),
        }
        self.published_messages.append(msg)
        return True
