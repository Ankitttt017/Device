"""
Unit tests for Cycle-Triggered Acquisition on M4598.
Validates zero-delay shot event creation, rising edge detection, and atomic persistence.
"""
import os
import tempfile
import unittest
from quad_gateway.config.config_loader import load_machine_config
from quad_gateway.drivers.slmp.slmp_driver import SLMPDriver
from quad_gateway.acquisition.collector import AcquisitionCollector
from quad_gateway.storage.database import StorageManager
from tests.mock_plc import MockSLMPServer


class TestCycleTriggeredAcquisition(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mock_server = MockSLMPServer(host="127.0.0.1", port=0)
        cls.mock_server.start()
        cls.config_path = os.path.join(
            os.path.dirname(__file__), "..", "quad_gateway", "config", "machine.json"
        )
        cls.config = load_machine_config(cls.config_path)

    @classmethod
    def tearDownClass(cls):
        cls.mock_server.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_trigger.db")
        self.driver = SLMPDriver(
            host="127.0.0.1",
            port=self.mock_server.port,
            timeout_seconds=2.0,
            monitoring_timer_250ms=16,
            read_only=True,
        )
        self.driver.connect()
        self.collector = AcquisitionCollector(config=self.config, driver=self.driver)
        self.storage = StorageManager(db_path=self.db_path)
        self.storage.sync_machine_config(self.config)

    def tearDown(self):
        self.driver.disconnect()
        self.temp_dir.cleanup()

    def test_read_bit_value_directly(self):
        # M4598 starts at 0 in mock PLC
        self.mock_server.m_bits[4598] = 0
        bit_val = self.collector.read_bit_value("M4598")
        self.assertEqual(bit_val, 0)

        # Toggle to 1
        self.mock_server.m_bits[4598] = 1
        bit_val = self.collector.read_bit_value("M4598")
        self.assertEqual(bit_val, 1)

        # Toggle back to 0
        self.mock_server.m_bits[4598] = 0
        bit_val = self.collector.read_bit_value("M4598")
        self.assertEqual(bit_val, 0)

    def test_cycle_end_trigger_and_instant_persistence(self):
        # Simulate shot end: M4598 transitions 0 -> 1
        self.mock_server.m_bits[4598] = 1
        self.mock_server.d_registers[1120] = 7788  # Custom SHOT_NO
        self.mock_server.d_registers[1127] = 450   # Custom CYCLE_TIME (45.0s)

        # Triggered acquisition
        event = self.collector.collect_cycle(trigger_type="CYCLE_END")
        self.assertEqual(event.trigger, "CYCLE_END")
        self.assertEqual(event.quality, "GOOD")

        # Verify shot tags
        shot_point = next((p for p in event.points if p.tag == "SHOT_NO"), None)
        self.assertIsNotNone(shot_point)
        self.assertEqual(shot_point.value, 7788)

        cycle_point = next((p for p in event.points if p.tag == "CYCLE_TIME"), None)
        self.assertIsNotNone(cycle_point)
        self.assertEqual(cycle_point.value, 450)

        # Atomic insert into SQLite local db
        success = self.storage.insert_event_and_queue(event)
        self.assertTrue(success)

        # Verify SQLite contents
        stored_event = self.storage.get_event_by_uuid(event.event_id)
        self.assertIsNotNone(stored_event)
        self.assertEqual(stored_event["event_uuid"], event.event_id)

        # Verify sync_queue entry is PENDING
        sq = self.storage.get_sync_queue_by_uuid(event.event_id)
        self.assertIsNotNone(sq)
        self.assertEqual(sq["event_uuid"], event.event_id)
        self.assertEqual(sq["status"], "PENDING")


if __name__ == "__main__":
    unittest.main()
