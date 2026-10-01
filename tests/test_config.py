"""
Unit tests for Machine Configuration and Batch Planning.
"""
import os
import unittest

from quad_gateway.config.config_loader import (
    ConfigurationError,
    load_machine_config,
    parse_device_address,
)
from quad_gateway.acquisition.collector import plan_word_batches


class TestConfigLoader(unittest.TestCase):
    """
    Validates machine.json loading and boundary rules.
    """

    def setUp(self):
        self.config_path = os.path.join(
            os.path.dirname(__file__), "..", "quad_gateway", "config", "machine.json"
        )

    def test_load_default_config(self):
        config = load_machine_config(self.config_path)
        self.assertEqual(config.machine_id, "UBE-850T-02")
        self.assertEqual(config.machine_name, "UBE 850 T - 02")
        self.assertEqual(config.host, "192.168.117.201")
        self.assertIn(config.port, (1026, 1027))
        self.assertEqual(config.protocol, "SLMP_3E_BINARY")
        self.assertTrue(config.read_only)
        self.assertEqual(len(config.registers), 60)

    def test_parse_device_address(self):
        dev_type, dev_num = parse_device_address("D1127")
        self.assertEqual(dev_type, "D")
        self.assertEqual(dev_num, 1127)

        dev_type, dev_num = parse_device_address("M840")
        self.assertEqual(dev_type, "M")
        self.assertEqual(dev_num, 840)

        with self.assertRaises(ConfigurationError):
            parse_device_address("INVALID_ADDR")

    def test_contiguous_batch_planning(self):
        config = load_machine_config(self.config_path)
        batches = plan_word_batches(config.registers)

        # Assert no batch contains registers from two different device types
        for b in batches:
            for r in b.registers:
                self.assertEqual(r.device_type, b.device_type)

        # Assert no unapproved memory gap inside a batch
        for b in batches:
            offset = 0
            for r in b.registers:
                expected_addr = b.start_address + offset
                self.assertEqual(
                    r.device_number,
                    expected_addr,
                    f"Batch has gap: register {r.tag} address {r.device_number} != expected {expected_addr}"
                )
                offset += r.words


if __name__ == "__main__":
    unittest.main()
