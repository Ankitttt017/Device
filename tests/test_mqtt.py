"""
Unit and Integration Tests for Phase 3: MQTT Publisher & Synchronization Manager.
Covers all 20 Phase-3 verification requirements without requiring an external broker.
"""
import io
import json
import logging
import os
import tempfile
import unittest
from datetime import datetime, timezone

from quad_gateway.acquisition.collector import AcquisitionCollector
from quad_gateway.config.config_loader import MQTTConfig, load_machine_config
from quad_gateway.drivers.slmp.slmp_driver import SLMPDriver
from quad_gateway.models.telemetry import AcquisitionEvent, TelemetryDataPoint
from quad_gateway.mqtt.publisher import FakeMQTTPublisher, MQTTPublisher
from quad_gateway.storage.database import StorageManager
from quad_gateway.sync.sync_manager import SyncManager, build_telemetry_topic
from tests.mock_plc import MockSLMPServer


class TestMQTTAndSyncManager(unittest.TestCase):
    """
    Comprehensive test suite covering MQTT configuration, topics, payload formatting,
    exponential backoff, offline buffering, and end-to-end telemetry synchronization.
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "mqtt_test.db")
        self.storage = StorageManager(db_path=self.db_path)

        self.machine_data = {
            "machine_id": "UBE-850T-02",
            "machine_name": "UBE 850 T - 02",
            "gateway_id": "QUAD-01",
            "plc_ip": "192.168.117.201",
            "plc_port": 5002,
            "protocol": "SLMP_3E_BINARY",
            "read_only": True,
            "enabled": True,
        }
        self.storage.insert_machine(self.machine_data)

        self.mqtt_config = MQTTConfig(
            enabled=True,
            host="127.0.0.1",
            port=1883,
            client_id="QUAD-01",
            keepalive=60,
            topic_prefix="quad",
            qos=1,
            retain=False,
            batch_size=2,
            retry_initial_delay=1.0,
            retry_max_delay=10.0,
            retry_backoff_factor=2.0,
        )
        self.publisher = FakeMQTTPublisher()
        self.sync_mgr = SyncManager(
            storage=self.storage,
            publisher=self.publisher,
            mqtt_config=self.mqtt_config,
            machine_id="UBE-850T-02",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def _create_sample_event(self, shot_no: int = 1042) -> AcquisitionEvent:
        return AcquisitionEvent(
            gateway_id="QUAD-01",
            machine_id="UBE-850T-02",
            points=[
                TelemetryDataPoint(
                    tag="SHOT_NO",
                    address="D1120",
                    raw_value=shot_no,
                    value=shot_no,
                    data_type="int16",
                    quality="GOOD",
                ),
                TelemetryDataPoint(
                    tag="CYCLE_TIME",
                    address="D1127",
                    raw_value=385,
                    value=385,
                    unit="sec",
                    data_type="decimal_scaled",
                    quality="GOOD",
                ),
            ],
            quality="GOOD",
        )

    # 1. MQTT configuration loads correctly
    def test_1_mqtt_config_loads_correctly(self):
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "quad_gateway", "config", "machine.json"
        )
        config = load_machine_config(config_path)
        self.assertIsNotNone(config.mqtt)
        self.assertTrue(config.mqtt.enabled)
        self.assertEqual(config.mqtt.port, 1883)
        self.assertEqual(config.mqtt.topic_prefix, "quad")
        self.assertEqual(config.mqtt.qos, 1)
        self.assertEqual(config.mqtt.batch_size, 50)

    # 2. MQTT topic generated correctly
    def test_2_mqtt_topic_generated_correctly(self):
        topic = build_telemetry_topic("quad", "UBE-850T-02")
        self.assertEqual(topic, "quad/UBE-850T-02/telemetry")
        topic_trailing = build_telemetry_topic("quad/", "UBE-850T-02")
        self.assertEqual(topic_trailing, "quad/UBE-850T-02/telemetry")

    # 3. Payload serialization works
    def test_3_payload_serialization_works(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)

        res = self.sync_mgr.sync_pending_events()
        self.assertEqual(res["published"], 1)
        self.assertEqual(len(self.publisher.published_messages), 1)

        msg = self.publisher.published_messages[0]
        self.assertIsInstance(msg["payload"], str)
        parsed = json.loads(msg["payload"])
        self.assertEqual(parsed["event_uuid"], event.event_id)
        self.assertIn("payload", parsed)
        self.assertIn("CYCLE_TIME", parsed["payload"])

    # 4. event_uuid preserved
    def test_4_event_uuid_preserved(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)
        self.sync_mgr.sync_pending_events()
        published = self.publisher.published_messages[0]["payload_dict"]
        self.assertEqual(published["event_uuid"], event.event_id)

    # 5. machine_id preserved
    def test_5_machine_id_preserved(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)
        self.sync_mgr.sync_pending_events()
        published = self.publisher.published_messages[0]["payload_dict"]
        self.assertEqual(published["machine_id"], "UBE-850T-02")

    # 6. gateway_id preserved
    def test_6_gateway_id_preserved(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)
        self.sync_mgr.sync_pending_events()
        published = self.publisher.published_messages[0]["payload_dict"]
        self.assertEqual(published["gateway_id"], "QUAD-01")

    # 7. CYCLE_TIME / D1127 preserved without guessing
    def test_7_cycle_time_d1127_preserved(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)
        self.sync_mgr.sync_pending_events()
        point = self.publisher.published_messages[0]["payload_dict"]["payload"]["CYCLE_TIME"]
        self.assertEqual(point["address"], "D1127")
        self.assertEqual(point["value"], 385)
        self.assertEqual(point["raw_value"], 385)
        self.assertEqual(point["unit"], "sec")
        self.assertEqual(point["data_type"], "decimal_scaled")

    # 8. PENDING events are retrieved
    def test_8_pending_events_are_retrieved(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)
        pending = self.storage.get_pending_sync_events()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["event_uuid"], event.event_id)
        self.assertEqual(pending[0]["status"], "PENDING")

    # 9. Oldest pending events are processed first (FIFO)
    def test_9_oldest_pending_events_processed_first(self):
        ev1 = self._create_sample_event(1001)
        ev2 = self._create_sample_event(1002)
        self.storage.insert_event_and_queue(ev1)
        self.storage.insert_event_and_queue(ev2)

        # Batch size is 1 -> should process ev1 first
        res = self.sync_mgr.sync_pending_events(batch_size=1)
        self.assertEqual(res["published"], 1)
        self.assertEqual(
            self.publisher.published_messages[0]["payload_dict"]["event_uuid"], ev1.event_id
        )

    # 10. MQTT publish success is detected and transitions status to SENDING
    def test_10_mqtt_publish_success_detected_and_marks_sending(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)
        res = self.sync_mgr.sync_pending_events()
        self.assertEqual(res["published"], 1)

        sq = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertEqual(sq["status"], "SENDING")
        self.assertEqual(sq["attempt_count"], 1)
        self.assertIsNotNone(sq["last_attempt_at"])

    # 11. MQTT publish failure is handled
    def test_11_mqtt_publish_failure_handled(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)

        # Force publisher failure
        self.publisher.fail_publish = True
        res = self.sync_mgr.sync_pending_events()
        self.assertEqual(res["failed"], 1)
        self.assertEqual(res["published"], 0)

        sq = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertEqual(sq["status"], "FAILED")
        self.assertEqual(sq["attempt_count"], 1)
        self.assertIsNotNone(sq["next_attempt_at"])

    # 12. Retry / backoff is scheduled
    def test_12_retry_backoff_scheduled(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)
        self.publisher.fail_publish = True

        self.sync_mgr.sync_pending_events()
        sq = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertIsNotNone(sq["next_attempt_at"])

        # Immediately attempting sync without elapsed time will skip this event
        res_skipped = self.sync_mgr.sync_pending_events()
        self.assertEqual(res_skipped["status"], "IDLE")
        self.assertEqual(res_skipped["attempted"], 0)

    # 13. MQTT connection failure does not crash acquisition
    def test_13_mqtt_connection_failure_does_not_crash_acquisition(self):
        # Offline publisher
        self.publisher.offline_mode = True

        # Simulate acquisition flow
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)

        # Sync manager runs safely without raising unhandled exception
        res = self.sync_mgr.sync_pending_events()
        self.assertEqual(res["status"], "OFFLINE")

    # 14. Pending events remain in SQLite when MQTT is unavailable
    def test_14_pending_events_remain_in_sqlite_when_mqtt_unavailable(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)

        self.publisher.offline_mode = True
        self.sync_mgr.sync_pending_events()

        # Event still securely in SQLite
        ev = self.storage.get_event_by_uuid(event.event_id)
        self.assertIsNotNone(ev)
        sq = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertIn(sq["status"], ("PENDING", "FAILED"))

    # 15. No event is marked SYNCED without server ACK
    def test_15_no_event_marked_synced_without_server_ack(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)

        self.sync_mgr.sync_pending_events()
        sq = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertNotEqual(sq["status"], "SYNCED")
        self.assertEqual(sq["status"], "SENDING")

    # 16. Duplicate UUID is preserved
    def test_16_duplicate_uuid_preserved(self):
        event = self._create_sample_event(1042)
        self.storage.insert_event_and_queue(event)
        self.sync_mgr.sync_pending_events()
        msg_uuid = self.publisher.published_messages[0]["payload_dict"]["event_uuid"]
        self.assertEqual(msg_uuid, event.event_id)

    # 17. Batch processing respects configured batch size
    def test_17_batch_processing_respects_configured_batch_size(self):
        for i in range(5):
            ev = self._create_sample_event(1000 + i)
            self.storage.insert_event_and_queue(ev)

        # batch_size is 2
        res = self.sync_mgr.sync_pending_events(batch_size=2)
        self.assertEqual(res["attempted"], 2)
        self.assertEqual(res["published"], 2)
        self.assertEqual(len(self.publisher.published_messages), 2)

    # 18. MQTT credentials are not written to logs
    def test_18_mqtt_credentials_not_written_to_logs(self):
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        mqtt_logger = logging.getLogger("quad_gateway.mqtt")
        mqtt_logger.addHandler(handler)

        secret_pass = "SuperSecretIndustrialPass123!"
        pub = MQTTPublisher(
            host="127.0.0.1",
            port=1883,
            username="operator1",
            password=secret_pass,
        )
        pub.start()
        pub.stop()

        log_output = log_capture.getvalue()
        self.assertNotIn(secret_pass, log_output)
        mqtt_logger.removeHandler(handler)

    # 19. Gateway can shut down cleanly
    def test_19_gateway_clean_shutdown(self):
        pub = FakeMQTTPublisher()
        pub.start()
        self.assertTrue(pub.is_connected)
        pub.stop()
        self.assertFalse(pub.is_connected)

    # 20. End-to-End: Mock PLC -> Collector -> SQLite -> Sync Manager -> Fake MQTT
    def test_20_end_to_end_mock_plc_to_sqlite_to_mqtt(self):
        mock_server = MockSLMPServer(host="127.0.0.1", port=0)
        mock_server.start()

        try:
            config_path = os.path.join(
                os.path.dirname(__file__), "..", "quad_gateway", "config", "machine.json"
            )
            config = load_machine_config(config_path)

            driver = SLMPDriver(
                host="127.0.0.1",
                port=mock_server.port,
                timeout_seconds=2.0,
                read_only=True,
            )
            collector = AcquisitionCollector(config=config, driver=driver)
            driver.connect()

            try:
                # 1. Acquire from Mock PLC
                event = collector.collect_cycle()
                self.assertEqual(event.quality, "GOOD")
                self.assertEqual(len(event.points), 60)

                # 2. Persist to SQLite
                self.storage.sync_machine_config(config)
                self.storage.insert_event_and_queue(event)

                # Verify stored as PENDING
                sq = self.storage.get_sync_queue_by_uuid(event.event_id)
                self.assertEqual(sq["status"], "PENDING")

                # 3. Synchronize to MQTT via FakeMQTTPublisher
                sync_res = self.sync_mgr.sync_pending_events()
                self.assertEqual(sync_res["published"], 1)

                # 4. Verify MQTT message
                self.assertEqual(len(self.publisher.published_messages), 1)
                pub_msg = self.publisher.published_messages[0]
                self.assertEqual(pub_msg["topic"], "quad/UBE-850T-02/telemetry")
                self.assertEqual(pub_msg["qos"], 1)

                payload = pub_msg["payload_dict"]
                self.assertEqual(payload["event_uuid"], event.event_id)
                self.assertEqual(payload["machine_id"], "UBE-850T-02")
                self.assertEqual(len(payload["payload"]), 60)
                self.assertEqual(payload["payload"]["CYCLE_TIME"]["value"], 385)
                self.assertEqual(payload["payload"]["PART_NAME"]["value"], "PART-UBE-850T02")
                self.assertEqual(payload["payload"]["SHOT_NO"]["value"], 1042)
                self.assertEqual(payload["payload"]["CYCLE_START"]["value"], 1)
                self.assertEqual(payload["payload"]["CYCLE_END"]["value"], 0)

                # 5. Verify SQLite sync queue state transitioned to SENDING
                sq_after = self.storage.get_sync_queue_by_uuid(event.event_id)
                self.assertEqual(sq_after["status"], "SENDING")
                self.assertEqual(sq_after["attempt_count"], 1)
                self.assertIsNone(sq_after["synced_at"])

            finally:
                driver.disconnect()

        finally:
            mock_server.stop()


if __name__ == "__main__":
    unittest.main()
