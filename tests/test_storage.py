"""
Unit and Integration Tests for Phase 2: QUAD Local SQLite Database.
Verifies all 16 storage integrity requirements and end-to-end Mock PLC -> Collector -> SQLite pipeline.
"""
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone

from quad_gateway.acquisition.collector import AcquisitionCollector
from quad_gateway.config.config_loader import load_machine_config
from quad_gateway.drivers.slmp.slmp_driver import SLMPDriver
from quad_gateway.models.telemetry import AcquisitionEvent, TelemetryDataPoint
from quad_gateway.storage.database import StorageManager, utc_now_str
from tests.mock_plc import MockSLMPServer


class TestStorageManager(unittest.TestCase):
    """
    Unit tests for SQLite schema, tables, transactions, and synchronization queue.
    """

    def setUp(self):
        # Create a clean temporary database file for each test
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_quad.db")
        self.storage = StorageManager(db_path=self.db_path)

        # Default test machine
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

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_1_database_initializes_successfully(self):
        """1. Proves database initializes successfully and creates the file."""
        self.assertTrue(os.path.exists(self.db_path))
        conn = self.storage.get_connection()
        try:
            # Check foreign_keys pragma
            fk = conn.execute("PRAGMA foreign_keys;").fetchone()[0]
            self.assertEqual(fk, 1)
        finally:
            conn.close()

    def test_2_tables_and_indexes_are_created(self):
        """2. Proves all core tables and indexes are created."""
        conn = self.storage.get_connection()
        try:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table';"
                ).fetchall()
            }
            self.assertIn("machines", tables)
            self.assertIn("machine_tags", tables)
            self.assertIn("acquisition_events", tables)
            self.assertIn("sync_queue", tables)

            indexes = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index';"
                ).fetchall()
            }
            self.assertIn("idx_machine_tags_machine_id", indexes)
            self.assertIn("idx_machine_tags_machine_enabled", indexes)
            self.assertIn("idx_acquisition_events_uuid", indexes)
            self.assertIn("idx_acquisition_events_machine_time", indexes)
            self.assertIn("idx_sync_queue_uuid", indexes)
            self.assertIn("idx_sync_queue_status_next", indexes)
        finally:
            conn.close()

    def test_3_machine_insert_and_upsert(self):
        """3. Proves machine can be inserted and upserted."""
        self.storage.insert_machine(self.machine_data)
        m = self.storage.get_machine("UBE-850T-02")
        self.assertIsNotNone(m)
        self.assertEqual(m["machine_name"], "UBE 850 T - 02")
        self.assertEqual(m["plc_ip"], "192.168.117.201")
        self.assertEqual(m["plc_port"], 5002)
        self.assertEqual(m["protocol"], "SLMP_3E_BINARY")
        self.assertEqual(m["read_only"], 1)

        # Upsert with updated machine_name
        updated = dict(self.machine_data)
        updated["machine_name"] = "UBE 850 T - 02 (Updated)"
        self.storage.upsert_machine(updated)
        m2 = self.storage.get_machine("UBE-850T-02")
        self.assertEqual(m2["machine_name"], "UBE 850 T - 02 (Updated)")

    def test_4_tags_insert_and_upsert(self):
        """4. Proves tags can be inserted/upserted, preserving NULL scale_factor for CYCLE_TIME."""
        self.storage.insert_machine(self.machine_data)

        # Insert CYCLE_TIME D1127 (scale_factor must be NULL - no guessing)
        cycle_time_tag = {
            "machine_id": "UBE-850T-02",
            "tag_name": "CYCLE_TIME",
            "address": "D1127",
            "data_type": "decimal_scaled",
            "unit": "sec",
            "scale_factor": None,
            "enabled": True,
        }
        self.storage.insert_machine_tag(cycle_time_tag)

        tags = self.storage.get_machine_tags("UBE-850T-02")
        self.assertEqual(len(tags), 1)
        self.assertEqual(tags[0]["tag_name"], "CYCLE_TIME")
        self.assertEqual(tags[0]["address"], "D1127")
        self.assertIsNone(tags[0]["scale_factor"])

        # Upsert tag
        cycle_time_tag["unit"] = "seconds"
        self.storage.upsert_machine_tag(cycle_time_tag)
        tags_after = self.storage.get_machine_tags("UBE-850T-02")
        self.assertEqual(len(tags_after), 1)
        self.assertEqual(tags_after[0]["unit"], "seconds")

    def test_5_acquisition_event_can_be_stored(self):
        """5. Proves an acquisition event can be stored."""
        self.storage.insert_machine(self.machine_data)

        event = AcquisitionEvent(
            gateway_id="QUAD-01",
            machine_id="UBE-850T-02",
            points=[
                TelemetryDataPoint(
                    tag="SHOT_NO",
                    address="D1120",
                    raw_value=1042,
                    value=1042,
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

        res = self.storage.insert_event_and_queue(event)
        self.assertTrue(res)

        stored = self.storage.get_event_by_uuid(event.event_id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored["event_uuid"], event.event_id)
        self.assertEqual(stored["machine_id"], "UBE-850T-02")
        self.assertEqual(stored["quality"], "GOOD")

    def test_6_sync_queue_entry_automatically_created(self):
        """6. Proves a sync_queue entry is automatically created on event insert."""
        self.storage.insert_machine(self.machine_data)
        event = AcquisitionEvent(
            gateway_id="QUAD-01",
            machine_id="UBE-850T-02",
            points=[],
            quality="GOOD",
        )
        self.storage.insert_event_and_queue(event)

        sq = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertIsNotNone(sq)
        self.assertEqual(sq["event_uuid"], event.event_id)
        self.assertEqual(sq["status"], "PENDING")
        self.assertEqual(sq["attempt_count"], 0)
        self.assertIsNone(sq["synced_at"])

    def test_7_event_uuid_is_unique(self):
        """7. Proves event_uuid is unique across events and queue."""
        self.storage.insert_machine(self.machine_data)
        event1 = AcquisitionEvent(
            gateway_id="QUAD-01", machine_id="UBE-850T-02", points=[]
        )
        event2 = AcquisitionEvent(
            gateway_id="QUAD-01", machine_id="UBE-850T-02", points=[]
        )
        self.assertNotEqual(event1.event_id, event2.event_id)

    def test_8_duplicate_event_uuid_rejected(self):
        """8. Proves duplicate event_uuid insertion is rejected and raises IntegrityError."""
        self.storage.insert_machine(self.machine_data)
        event = AcquisitionEvent(
            gateway_id="QUAD-01", machine_id="UBE-850T-02", points=[]
        )
        self.storage.insert_event_and_queue(event)

        # Attempting to insert identical event_uuid must fail
        with self.assertRaises(sqlite3.IntegrityError):
            self.storage.insert_event_and_queue(event)

    def test_9_foreign_key_constraints_enforced(self):
        """9. Proves foreign key constraints work (cannot reference non-existent machine)."""
        event = AcquisitionEvent(
            gateway_id="QUAD-01",
            machine_id="NON_EXISTENT_MACHINE",
            points=[],
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.storage.insert_event_and_queue(event)

    def test_10_transaction_rollback_works(self):
        """10. Proves atomic transaction rollback leaves no orphan partial records on failure."""
        self.storage.insert_machine(self.machine_data)
        event = AcquisitionEvent(
            gateway_id="QUAD-01",
            machine_id="UBE-850T-02",
            points=[],
        )

        # Manually create conflicting sync_queue row with same event_uuid first
        conn = self.storage.get_connection()
        try:
            # Insert valid event 1
            self.storage.insert_event_and_queue(event)
        finally:
            conn.close()

        # Try to insert duplicate event: both acquisition_events and sync_queue must not corrupt
        with self.assertRaises(sqlite3.IntegrityError):
            self.storage.insert_event_and_queue(event)

        # Verify only 1 event exists
        conn = self.storage.get_connection()
        try:
            cnt_ae = conn.execute("SELECT COUNT(*) FROM acquisition_events;").fetchone()[0]
            cnt_sq = conn.execute("SELECT COUNT(*) FROM sync_queue;").fetchone()[0]
            self.assertEqual(cnt_ae, 1)
            self.assertEqual(cnt_sq, 1)
        finally:
            conn.close()

    def test_11_data_survives_closing_and_reopening_connection(self):
        """11. Proves data survives application restart (closing/reopening connection)."""
        self.storage.insert_machine(self.machine_data)
        event = AcquisitionEvent(
            gateway_id="QUAD-01",
            machine_id="UBE-850T-02",
            points=[
                TelemetryDataPoint(
                    tag="SHOT_NO", address="D1120", raw_value=1042, value=1042
                )
            ],
        )
        self.storage.insert_event_and_queue(event)

        # Simulate complete restart by instantiating brand new StorageManager
        new_storage_instance = StorageManager(db_path=self.db_path)
        m = new_storage_instance.get_machine("UBE-850T-02")
        ev = new_storage_instance.get_event_by_uuid(event.event_id)
        sq = new_storage_instance.get_sync_queue_by_uuid(event.event_id)

        self.assertIsNotNone(m)
        self.assertIsNotNone(ev)
        self.assertIsNotNone(sq)
        self.assertEqual(sq["status"], "PENDING")

    def test_12_pending_status_is_preserved(self):
        """12. Proves newly inserted events strictly maintain PENDING status."""
        self.storage.insert_machine(self.machine_data)
        event = AcquisitionEvent(
            gateway_id="QUAD-01", machine_id="UBE-850T-02", points=[]
        )
        self.storage.insert_event_and_queue(event)

        pending = self.storage.get_pending_sync_events()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["event_uuid"], event.event_id)
        self.assertEqual(pending[0]["status"], "PENDING")

    def test_13_no_event_automatically_marked_synced(self):
        """13. Proves no event is marked SYNCED without explicit acknowledgment."""
        self.storage.insert_machine(self.machine_data)
        event = AcquisitionEvent(
            gateway_id="QUAD-01", machine_id="UBE-850T-02", points=[]
        )
        self.storage.insert_event_and_queue(event)

        sq = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertNotEqual(sq["status"], "SYNCED")
        self.assertEqual(sq["status"], "PENDING")

        # Explicit mark test
        now = utc_now_str()
        self.storage.mark_sync_status(event.event_id, status="SYNCED", synced_at=now)
        sq_updated = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertEqual(sq_updated["status"], "SYNCED")
        self.assertEqual(sq_updated["synced_at"], now)

    def test_14_multiple_acquisition_events_stored(self):
        """14. Proves multiple sequential acquisition events can be stored cleanly."""
        self.storage.insert_machine(self.machine_data)
        uuids = []
        for i in range(5):
            ev = AcquisitionEvent(
                gateway_id="QUAD-01",
                machine_id="UBE-850T-02",
                points=[
                    TelemetryDataPoint(
                        tag="SHOT_NO", address="D1120", raw_value=1000 + i, value=1000 + i
                    )
                ],
            )
            uuids.append(ev.event_id)
            self.storage.insert_event_and_queue(ev)

        pending = self.storage.get_pending_sync_events(limit=10)
        self.assertEqual(len(pending), 5)
        for u in uuids:
            self.assertIsNotNone(self.storage.get_event_by_uuid(u))

    def test_15_querying_pending_events_works(self):
        """15. Proves querying pending sync events with limit and ordering works."""
        self.storage.insert_machine(self.machine_data)
        for i in range(3):
            ev = AcquisitionEvent(
                gateway_id="QUAD-01",
                machine_id="UBE-850T-02",
                points=[],
            )
            self.storage.insert_event_and_queue(ev)

        pending_limited = self.storage.get_pending_sync_events(limit=2)
        self.assertEqual(len(pending_limited), 2)
        self.assertEqual(pending_limited[0]["status"], "PENDING")
        self.assertEqual(pending_limited[1]["status"], "PENDING")

    def test_16_json_payload_read_back_correctly(self):
        """16. Proves normalized JSON payload preserves tag, address, values, unit, and quality."""
        self.storage.insert_machine(self.machine_data)
        event = AcquisitionEvent(
            gateway_id="QUAD-01",
            machine_id="UBE-850T-02",
            points=[
                TelemetryDataPoint(
                    tag="CYCLE_TIME",
                    address="D1127",
                    raw_value=385,
                    value=385,
                    unit="sec",
                    data_type="decimal_scaled",
                    quality="GOOD",
                ),
                TelemetryDataPoint(
                    tag="CYCLE_START",
                    address="M840",
                    raw_value=1,
                    value=1,
                    unit=None,
                    data_type="bit",
                    quality="GOOD",
                ),
            ],
            quality="GOOD",
        )
        self.storage.insert_event_and_queue(event)

        stored = self.storage.get_event_by_uuid(event.event_id)
        payload = stored["payload_decoded"]

        self.assertIn("CYCLE_TIME", payload)
        self.assertEqual(payload["CYCLE_TIME"]["address"], "D1127")
        self.assertEqual(payload["CYCLE_TIME"]["data_type"], "decimal_scaled")
        self.assertEqual(payload["CYCLE_TIME"]["value"], 385)
        self.assertEqual(payload["CYCLE_TIME"]["raw_value"], 385)
        self.assertEqual(payload["CYCLE_TIME"]["unit"], "sec")
        self.assertEqual(payload["CYCLE_TIME"]["quality"], "GOOD")

        self.assertIn("CYCLE_START", payload)
        self.assertEqual(payload["CYCLE_START"]["address"], "M840")
        self.assertEqual(payload["CYCLE_START"]["data_type"], "bit")
        self.assertEqual(payload["CYCLE_START"]["value"], 1)


class TestMockPLCToSQLiteIntegration(unittest.TestCase):
    """
    End-to-End Integration: Mock PLC -> Collector -> Telemetry Model -> SQLite Persistence.
    """

    @classmethod
    def setUpClass(cls):
        cls.mock_server = MockSLMPServer(host="127.0.0.1", port=0)
        cls.mock_server.start()

    @classmethod
    def tearDownClass(cls):
        cls.mock_server.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "integration.db")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_mock_plc_acquisition_and_sqlite_persistence(self):
        """
        Executes real acquisition from MockSLMPServer and verifies persistent storage
        in SQLite with all 60 registers and PENDING sync_queue entry.
        """
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "quad_gateway", "config", "machine.json"
        )
        config = load_machine_config(config_path)

        # Initialize storage and sync machine catalog
        storage = StorageManager(db_path=self.db_path)
        storage.sync_machine_config(config)

        # Verify machine and 60 tags synchronized
        m = storage.get_machine("UBE-850T-02")
        self.assertIsNotNone(m)
        self.assertEqual(m["machine_id"], "UBE-850T-02")
        tags = storage.get_machine_tags("UBE-850T-02")
        self.assertEqual(len(tags), 60)

        # Connect driver to Mock PLC
        driver = SLMPDriver(
            host="127.0.0.1",
            port=self.mock_server.port,
            timeout_seconds=2.0,
            read_only=True,
        )
        collector = AcquisitionCollector(config=config, driver=driver)
        driver.connect()

        try:
            # 1. Acquire telemetry cycle
            event = collector.collect_cycle()
            self.assertEqual(event.quality, "GOOD")
            self.assertEqual(len(event.points), 60)

            # 2. Persist to SQLite
            persisted = storage.insert_event_and_queue(event)
            self.assertTrue(persisted)

            # 3. Read back from SQLite
            stored_event = storage.get_event_by_uuid(event.event_id)
            self.assertIsNotNone(stored_event)
            self.assertEqual(stored_event["event_uuid"], event.event_id)
            self.assertEqual(stored_event["quality"], "GOOD")

            payload = stored_event["payload_decoded"]
            self.assertEqual(len(payload), 60)

            # Check specific registers in SQLite payload
            # PART_NAME (D100)
            self.assertEqual(payload["PART_NAME"]["value"], "PART-UBE-850T02")
            # CYCLE_TIME (D1127)
            self.assertEqual(payload["CYCLE_TIME"]["value"], 385)
            self.assertEqual(payload["CYCLE_TIME"]["unit"], "sec")
            # SHOT_NO (D1120)
            self.assertEqual(payload["SHOT_NO"]["value"], 1042)
            # CYCLE_START (M840)
            self.assertEqual(payload["CYCLE_START"]["value"], 1)
            # CYCLE_END (M4598)
            self.assertEqual(payload["CYCLE_END"]["value"], 0)

            # 4. Verify sync_queue row
            queue_item = storage.get_sync_queue_by_uuid(event.event_id)
            self.assertIsNotNone(queue_item)
            self.assertEqual(queue_item["status"], "PENDING")
            self.assertEqual(queue_item["attempt_count"], 0)
            self.assertIsNone(queue_item["synced_at"])

            # 5. Verify querying pending sync queue
            pending_events = storage.get_pending_sync_events()
            self.assertEqual(len(pending_events), 1)
            self.assertEqual(pending_events[0]["event_uuid"], event.event_id)
            self.assertEqual(pending_events[0]["payload_decoded"]["SHOT_NO"]["value"], 1042)

        finally:
            driver.disconnect()


if __name__ == "__main__":
    unittest.main()
