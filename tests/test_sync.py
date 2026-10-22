"""
Tests for server synchronization: ACK handling, resend of unACKed events,
retention cleanup and the shot identity block sent to the Main Server.
"""
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

from quad_gateway.config.config_loader import ConfigurationError, MQTTConfig, load_machine_config
from quad_gateway.models.telemetry import AcquisitionEvent, TelemetryDataPoint
from quad_gateway.mqtt.publisher import FakeMQTTPublisher, MQTTPublisher
from quad_gateway.storage.database import StorageManager
from quad_gateway.sync.sync_manager import SyncManager, build_ack_topic, build_shot_identity

ACK_TOPIC = "quad/UBE-850T-02/ack"
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "quad_gateway", "config", "machine.json")


def point(tag, value, address="D0", quality="GOOD"):
    return TelemetryDataPoint(tag=tag, address=address, raw_value=value, value=value, quality=quality)


def make_event(shot_no=2710, status=1):
    return AcquisitionEvent(
        gateway_id="QUAD-01",
        machine_id="UBE-850T-02",
        points=[
            point("SHOT_NO", shot_no, "D1120"),
            point("SHOT_STATUS", status, "D1301"),
            point("PART_NAME", "OPK12-S17", "D100"),
            point("SHOT_YEAR", 26, "D2100"),
            point("SHOT_MONTH", 10, "D2101"),
            point("SHOT_DAY", 21, "D2102"),
            point("SHOT_HOUR", 16, "D2103"),
            point("SHOT_MINUTE", 10, "D2104"),
            point("SHOT_SECOND", 15, "D2105"),
        ],
    )


class TestServerSync(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "sync_test.db")
        self.storage = StorageManager(db_path=self.db_path)
        self.storage.sync_machine_config(load_machine_config(CONFIG_PATH))
        self.mqtt_config = MQTTConfig(batch_size=50, retry_max_delay=10.0, ack_timeout_seconds=300.0)
        self.publisher = FakeMQTTPublisher()
        self.sync = SyncManager(self.storage, self.publisher, self.mqtt_config, "UBE-850T-02")

    def tearDown(self):
        self.temp_dir.cleanup()

    def queue_status(self, event):
        return self.storage.get_sync_queue_by_uuid(event.event_id)["status"]

    def publish_events(self, count=1):
        events = [make_event(2700 + i) for i in range(count)]
        for e in events:
            self.storage.insert_event_and_queue(e)
        self.sync.run_cycle()
        return events

    def backdate(self, column, event, seconds):
        old = (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()
        conn = sqlite3.connect(self.db_path)
        try:
            with conn:
                conn.execute(f"UPDATE sync_queue SET {column} = ? WHERE event_uuid = ?", (old, event.event_id))
        finally:
            conn.close()

    # --- ACK handling -------------------------------------------------------

    def test_subscribes_to_machine_ack_topic(self):
        self.assertEqual(build_ack_topic("quad/", "UBE-850T-02"), ACK_TOPIC)
        self.assertIn(ACK_TOPIC, self.publisher.subscriptions)

    def test_saved_ack_marks_event_synced(self):
        (event,) = self.publish_events()
        self.assertEqual(self.queue_status(event), "SENDING")
        self.publisher.deliver(ACK_TOPIC, json.dumps({"event_uuid": event.event_id, "status": "SAVED"}))
        result = self.sync.run_cycle()
        self.assertEqual(result["acked"], 1)
        row = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertEqual(row["status"], "SYNCED")
        self.assertIsNotNone(row["synced_at"])

    def test_batch_and_duplicate_acks_mark_all_synced(self):
        events = self.publish_events(3)
        self.publisher.deliver(ACK_TOPIC, json.dumps({"event_uuids": [e.event_id for e in events[:2]]}))
        self.publisher.deliver(ACK_TOPIC, json.dumps({"event_uuid": events[2].event_id, "status": "DUPLICATE"}))
        self.sync.process_acks()
        self.assertEqual([self.queue_status(e) for e in events], ["SYNCED"] * 3)

    def test_error_ack_puts_event_back_for_retry(self):
        (event,) = self.publish_events()
        self.publisher.deliver(ACK_TOPIC, json.dumps({"event_uuid": event.event_id, "status": "ERROR", "error": "db down"}))
        self.sync.process_acks()
        row = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertEqual(row["status"], "FAILED")
        self.assertIn("db down", row["last_error"])
        self.assertIsNotNone(row["next_attempt_at"])

    def test_malformed_ack_is_ignored(self):
        (event,) = self.publish_events()
        self.publisher.deliver(ACK_TOPIC, "not json")
        self.publisher.deliver(ACK_TOPIC, json.dumps({"status": "SAVED"}))
        self.assertEqual(self.sync.process_acks(), {"synced": 0, "rejected": 0})
        self.assertEqual(self.queue_status(event), "SENDING")

    def test_ack_for_unknown_uuid_changes_nothing(self):
        self.publisher.deliver(ACK_TOPIC, json.dumps({"event_uuid": "no-such-event"}))
        self.assertEqual(self.sync.process_acks()["synced"], 0)

    # --- Resend ---------------------------------------------------------------

    def test_unacked_event_is_resent_after_timeout(self):
        (event,) = self.publish_events()
        self.assertEqual(len(self.publisher.published_messages), 1)
        self.backdate("last_attempt_at", event, 301)
        result = self.sync.run_cycle()
        self.assertEqual(result["requeued"], 1)
        self.assertEqual(len(self.publisher.published_messages), 2)
        self.assertEqual(self.publisher.published_messages[1]["payload_dict"]["event_uuid"], event.event_id)
        self.assertEqual(self.queue_status(event), "SENDING")

    def test_recent_or_acked_events_are_not_resent(self):
        recent, acked = self.publish_events(2)
        self.publisher.deliver(ACK_TOPIC, json.dumps({"event_uuid": acked.event_id}))
        self.sync.process_acks()
        self.backdate("last_attempt_at", acked, 3600)
        self.assertEqual(self.sync.run_cycle()["requeued"], 0)
        self.assertEqual(len(self.publisher.published_messages), 2)

    def test_offline_broker_keeps_events_and_sends_later(self):
        self.publisher.offline_mode = True
        (event,) = self.publish_events()
        self.assertEqual(self.queue_status(event), "PENDING")
        self.publisher.offline_mode = False
        self.sync.run_cycle()
        self.assertEqual(self.queue_status(event), "SENDING")

    # --- Retention ------------------------------------------------------------

    def test_purge_removes_only_old_synced_events(self):
        old_synced, new_synced, unacked = self.publish_events(3)
        self.publisher.deliver(ACK_TOPIC, json.dumps({"event_uuids": [old_synced.event_id, new_synced.event_id]}))
        self.sync.process_acks()
        self.backdate("synced_at", old_synced, 31 * 86400)
        self.backdate("last_attempt_at", unacked, 31 * 86400)

        self.assertEqual(self.sync.purge_synced(retention_days=30), 1)
        self.assertIsNone(self.storage.get_event_by_uuid(old_synced.event_id))
        self.assertIsNone(self.storage.get_sync_queue_by_uuid(old_synced.event_id))
        self.assertIsNotNone(self.storage.get_event_by_uuid(new_synced.event_id))
        self.assertIsNotNone(self.storage.get_event_by_uuid(unacked.event_id))

    def test_status_counts(self):
        events = self.publish_events(2)
        self.publisher.deliver(ACK_TOPIC, json.dumps({"event_uuid": events[0].event_id}))
        self.sync.process_acks()
        self.assertEqual(self.storage.get_sync_status_counts(), {"SENDING": 1, "SYNCED": 1})

    # --- Message content ------------------------------------------------------

    def test_message_carries_shot_identity(self):
        self.publish_events()
        msg = self.publisher.published_messages[0]["payload_dict"]
        self.assertEqual(msg["shot"], {
            "shot_no": 2700, "shot_time": "2026-10-21 16:10:15", "result": "OK", "part_name": "OPK12-S17",
        })

    def test_shot_identity_never_guesses(self):
        payload = {p.tag: p.to_dict() for p in make_event(status=5).points}
        payload["SHOT_SECOND"]["quality"] = "BAD"
        identity = build_shot_identity(payload)
        self.assertIsNone(identity["shot_time"])
        self.assertEqual(identity["result"], "NG")
        payload["SHOT_RESULT"] = {"value": "WARM_UP", "quality": "GOOD"}
        self.assertEqual(build_shot_identity(payload)["result"], "WARM_UP")


class TestMQTTPublisherSubscriptions(unittest.TestCase):
    """Subscription handling of the real paho-based publisher, using a stand-in client."""

    def setUp(self):
        self.publisher = MQTTPublisher(qos=1)
        self.client = mock.Mock()
        self.received = []
        self.publisher.subscribe(ACK_TOPIC, lambda topic, text: self.received.append((topic, text)))

    def test_resubscribes_after_every_connect(self):
        self.publisher._on_connect(self.client, None, None, 0)
        self.publisher._on_connect(self.client, None, None, 0)
        self.assertEqual(self.client.subscribe.call_args_list, [mock.call(ACK_TOPIC, qos=1)] * 2)

    def test_dispatches_message_to_callback(self):
        msg = SimpleNamespace(topic=ACK_TOPIC, payload=b'{"event_uuid": "x"}')
        self.publisher._on_message(self.client, None, msg)
        self.assertEqual(self.received, [(ACK_TOPIC, '{"event_uuid": "x"}')])

    def test_callback_error_does_not_crash_network_thread(self):
        self.publisher.subscribe(ACK_TOPIC, mock.Mock(side_effect=RuntimeError("boom")))
        self.publisher._on_message(self.client, None, SimpleNamespace(topic=ACK_TOPIC, payload=b"{}"))


class TestSyncConfig(unittest.TestCase):

    def test_machine_json_sync_settings(self):
        config = load_machine_config(CONFIG_PATH)
        self.assertEqual(config.mqtt.host, "192.168.100.136")
        self.assertEqual(config.mqtt.ack_timeout_seconds, 300)
        self.assertEqual(config.mqtt.sync_interval_seconds, 5)
        self.assertEqual(config.retention_days, 30)
        tags = {r.tag for r in config.registers}
        self.assertNotIn("HIGH_SHOT_COUNT", tags)
        self.assertNotIn("NG_COUNTER", tags)

    def test_credentials_from_environment(self):
        with mock.patch.dict(os.environ, {"MQTT_USERNAME": "quad", "MQTT_PASSWORD": "secret"}):
            config = load_machine_config(CONFIG_PATH)
        self.assertEqual((config.mqtt.username, config.mqtt.password), ("quad", "secret"))

    def test_invalid_retention_rejected(self):
        with open(CONFIG_PATH, encoding="utf-8") as f:
            data = json.load(f)
        data["storage"]["retention_days"] = 0
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "machine.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f)
            with self.assertRaises(ConfigurationError):
                load_machine_config(path)


class TestFlowControl(unittest.TestCase):
    """Publishing pauses while max_unacked events await a server ACK."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.storage = StorageManager(db_path=os.path.join(self.temp_dir.name, "flow.db"))
        self.storage.sync_machine_config(load_machine_config(CONFIG_PATH))
        self.publisher = FakeMQTTPublisher()
        self.sync = SyncManager(self.storage, self.publisher, MQTTConfig(batch_size=50, max_unacked=5), "UBE-850T-02")
        self.events = [make_event(3000 + i) for i in range(8)]
        for e in self.events:
            self.storage.insert_event_and_queue(e)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_stops_at_limit_without_acks(self):
        self.assertEqual(self.sync.run_cycle()["published"], 5)
        result = self.sync.run_cycle()
        self.assertEqual((result["status"], result["published"]), ("WAITING_ACK", 0))
        self.assertEqual(self.storage.get_sync_status_counts(), {"SENDING": 5, "PENDING": 3})

    def test_acks_free_room_for_more(self):
        self.sync.run_cycle()
        first_two = [m["payload_dict"]["event_uuid"] for m in self.publisher.published_messages[:2]]
        self.publisher.deliver(ACK_TOPIC, json.dumps({"event_uuids": first_two}))
        self.assertEqual(self.sync.run_cycle()["published"], 2)
        self.assertEqual(self.storage.get_sync_status_counts(), {"SENDING": 5, "SYNCED": 2, "PENDING": 1})


class TestStatusLastWill(unittest.TestCase):
    """Retained online/offline status on quad/<machine>/status."""

    STATUS_TOPIC = "quad/UBE-850T-02/status"

    def test_status_topic_name(self):
        from quad_gateway.sync.sync_manager import build_status_topic
        self.assertEqual(build_status_topic("quad", "UBE-850T-02"), self.STATUS_TOPIC)

    def test_online_published_retained_on_connect(self):
        publisher = MQTTPublisher(client_id="QUAD-01", status_topic=self.STATUS_TOPIC)
        client = mock.Mock()
        publisher._on_connect(client, None, None, 0)
        client.publish.assert_called_once_with(
            self.STATUS_TOPIC, json.dumps({"status": "online", "gateway_id": "QUAD-01"}), qos=1, retain=True
        )

    def test_last_will_and_clean_offline(self):
        publisher = MQTTPublisher(client_id="QUAD-01", status_topic=self.STATUS_TOPIC)
        client = mock.Mock()
        fake_client_module = SimpleNamespace(
            Client=mock.Mock(return_value=client), CallbackAPIVersion=SimpleNamespace(VERSION2=2)
        )
        fake_mqtt_pkg = SimpleNamespace(client=fake_client_module)
        modules = {
            "paho": SimpleNamespace(mqtt=fake_mqtt_pkg),
            "paho.mqtt": fake_mqtt_pkg,
            "paho.mqtt.client": fake_client_module,
        }
        with mock.patch.dict("sys.modules", modules):
            publisher.start()
        offline = json.dumps({"status": "offline", "gateway_id": "QUAD-01"})
        client.will_set.assert_called_once_with(self.STATUS_TOPIC, offline, qos=1, retain=True)

        publisher._connected = True
        publisher.stop()
        client.publish.assert_called_once_with(self.STATUS_TOPIC, offline, qos=1, retain=True)
        client.disconnect.assert_called_once()

    def test_no_status_messages_without_topic(self):
        publisher = MQTTPublisher()
        client = mock.Mock()
        publisher._on_connect(client, None, None, 0)
        client.publish.assert_not_called()


class TestDeploymentFiles(unittest.TestCase):

    def test_service_loads_private_credentials_file(self):
        scripts = os.path.join(os.path.dirname(__file__), "..", "scripts")
        for name in ("quad_gateway.service", "install_service.sh"):
            with open(os.path.join(scripts, name), encoding="utf-8") as f:
                self.assertIn("EnvironmentFile=-", f.read(), name)

    def test_report_scaling_for_cycle_time_and_pressures(self):
        scales = {r.tag: r.scale_factor for r in load_machine_config(CONFIG_PATH).registers}
        self.assertEqual(scales["CYCLE_TIME"], 0.1)
        self.assertEqual(scales["SHOT_ACC_PRESSURE"], 0.01)
        self.assertEqual(scales["INTENSIFICATION_ACC_PRESSURE"], 0.01)


if __name__ == "__main__":
    unittest.main()
