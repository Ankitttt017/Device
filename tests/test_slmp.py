"""
Unit and Integration Tests for SLMP Driver and Protocol Engine.
Verifies frame building, response parsing, decoders, read-only safety,
and integration with Mock SLMP PLC server.
"""
import os
import struct
import unittest

from quad_gateway.config.config_loader import load_machine_config
from quad_gateway.acquisition.collector import AcquisitionCollector
from quad_gateway.drivers.slmp.decoder import (
    decode_bit,
    decode_int16,
    decode_scaled_decimal,
    decode_string,
    decode_uint16,
)
from quad_gateway.drivers.slmp.slmp_driver import (
    SLMPDriver,
    SLMPDriverError,
)
from quad_gateway.drivers.slmp.slmp_protocol import (
    SLMPProtocolError,
    build_read_bits_frame,
    build_read_words_frame,
    parse_bits_response,
    parse_response_frame,
    parse_words_response,
)
from tests.mock_plc import MockSLMPServer


class TestSLMPProtocol(unittest.TestCase):
    """
    Tests for pure binary SLMP 3E frame construction and parsing.
    """

    def test_build_read_words_frame(self):
        # Read D1127, 1 word
        frame = build_read_words_frame(
            device_type="D",
            head_device_number=1127,
            points=1,
            monitoring_timer_250ms=16
        )
        self.assertEqual(len(frame), 21)
        # Subheader: 0x0050 (little endian: 0x50, 0x00)
        self.assertEqual(frame[:2], b"\x50\x00")
        # Command: 0x0401 (offset 11: 0x01, 0x04)
        self.assertEqual(frame[11:13], b"\x01\x04")
        # Subcommand: 0x0000 (offset 13: 0x00, 0x00)
        self.assertEqual(frame[13:15], b"\x00\x00")
        # Head device number: 1127 = 0x000467 -> 0x67, 0x04, 0x00
        self.assertEqual(frame[15:18], b"\x67\x04\x00")
        # Device code: D = 0xA8
        self.assertEqual(frame[18], 0xA8)
        # Points: 1 = 0x0001
        self.assertEqual(frame[19:21], b"\x01\x00")

    def test_build_read_bits_frame(self):
        # Read M840, 1 bit
        frame = build_read_bits_frame(
            device_type="M",
            head_device_number=840,
            points=1,
            monitoring_timer_250ms=16
        )
        self.assertEqual(len(frame), 21)
        # Subcommand: 0x0001 (offset 13: 0x01, 0x00)
        self.assertEqual(frame[13:15], b"\x01\x00")
        # Head device: 840 = 0x000348 -> 0x48, 0x03, 0x00
        self.assertEqual(frame[15:18], b"\x48\x03\x00")
        # Device code: M = 0x90
        self.assertEqual(frame[18], 0x90)

    def test_parse_response_frame_success(self):
        # Subheader(2) + Net(1) + Station(1) + IO(2) + Multi(1) + Len(2) + EndCode(2) + Payload
        payload = b"\x12\x34"  # 1 word
        length = 2 + len(payload)
        frame = (
            struct.pack("<H", 0x00D0)
            + bytes([0x00, 0xFF])
            + struct.pack("<H", 0x03FF)
            + bytes([0x00])
            + struct.pack("<H", length)
            + struct.pack("<H", 0x0000)  # End code success
            + payload
        )
        extracted = parse_response_frame(frame)
        self.assertEqual(extracted, payload)

    def test_parse_response_frame_error(self):
        # End code 0xC059 (Error)
        frame = (
            struct.pack("<H", 0x00D0)
            + bytes([0x00, 0xFF])
            + struct.pack("<H", 0x03FF)
            + bytes([0x00])
            + struct.pack("<H", 2)
            + struct.pack("<H", 0xC059)
        )
        with self.assertRaises(SLMPProtocolError) as ctx:
            parse_response_frame(frame)
        self.assertEqual(ctx.exception.end_code, 0xC059)

    def test_parse_words_response(self):
        payload = struct.pack("<HH", 100, 200)
        words = parse_words_response(payload, expected_points=2)
        self.assertEqual(words, [100, 200])

    def test_parse_bits_response(self):
        # 1 byte with 2 points: P1 ON (high nibble), P2 OFF (low nibble) -> 0x10
        payload = bytes([0x10])
        bits = parse_bits_response(payload, expected_points=2)
        self.assertEqual(bits, [1, 0])


class TestDecoders(unittest.TestCase):
    """
    Tests for register value decoders.
    """

    def test_decode_int16(self):
        self.assertEqual(decode_int16(struct.pack("<h", 1250)), 1250)
        self.assertEqual(decode_int16(struct.pack("<h", -500)), -500)
        self.assertEqual(decode_int16(struct.pack("<h", 0)), 0)

    def test_decode_uint16(self):
        self.assertEqual(decode_uint16(struct.pack("<H", 65000)), 65000)

    def test_decode_scaled_decimal(self):
        # No scaling
        raw, scaled = decode_scaled_decimal(125, None)
        self.assertEqual(raw, 125)
        self.assertEqual(scaled, 125)

        # Scale by 0.1
        raw, scaled = decode_scaled_decimal(125, 0.1)
        self.assertEqual(raw, 125)
        self.assertEqual(scaled, 12.5)

    def test_decode_string_little_endian(self):
        # "AB" stored little-endian: low byte 'A', high byte 'B'
        raw = b"PART-01\x00\x00\x00"
        decoded = decode_string(raw, byte_order="little")
        self.assertEqual(decoded, "PART-01")

    def test_decode_string_big_endian(self):
        # "BA" in bytes swapped to "AB"
        raw = b"APTRO-\x001"
        # When decoded as big endian, adjacent bytes swap
        decoded = decode_string(b"BATR", byte_order="big")
        self.assertEqual(decoded, "ABRT")

    def test_decode_bit(self):
        self.assertEqual(decode_bit(1), 1)
        self.assertEqual(decode_bit(0), 0)
        self.assertEqual(decode_bit(True), 1)
        self.assertEqual(decode_bit(False), 0)


class TestReadOnlySafety(unittest.TestCase):
    """
    Verifies that the SLMP driver cannot be configured for write operations
    and has no write methods.
    """

    def test_safety_check_on_init(self):
        with self.assertRaises(SLMPDriverError):
            SLMPDriver(host="127.0.0.1", port=5002, read_only=False)

    def test_no_write_methods_exist(self):
        driver = SLMPDriver(host="127.0.0.1", port=5002, read_only=True)
        # Check that no write methods exist
        write_attrs = [attr for attr in dir(driver) if "write" in attr.lower()]
        self.assertEqual(write_attrs, [], f"Write methods found on driver: {write_attrs}")


class TestEndToEndMockAcquisition(unittest.TestCase):
    """
    End-to-end integration test acquiring all 60 registers from MockSLMPServer.
    """

    @classmethod
    def setUpClass(cls):
        cls.mock_server = MockSLMPServer(host="127.0.0.1", port=0)
        cls.mock_server.start()

    @classmethod
    def tearDownClass(cls):
        cls.mock_server.stop()

    def test_full_cycle_acquisition(self):
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "quad_gateway", "config", "machine.json"
        )
        config = load_machine_config(config_path)

        driver = SLMPDriver(
            host="127.0.0.1",
            port=self.mock_server.port,
            timeout_seconds=2.0,
            read_only=True
        )

        collector = AcquisitionCollector(config=config, driver=driver)
        driver.connect()

        try:
            event = collector.collect_cycle()
            self.assertEqual(event.quality, "GOOD")
            self.assertEqual(len(event.points), len(config.registers))

            # Validate specific known registers
            points_by_tag = {p.tag: p for p in event.points}

            # 1. Part name string
            part_name = points_by_tag["PART_NAME"]
            self.assertEqual(part_name.value, "PART-UBE-850T02")
            self.assertEqual(part_name.quality, "GOOD")

            # 2. Cycle Start (bit)
            cycle_start = points_by_tag["CYCLE_START"]
            self.assertEqual(cycle_start.value, 1)

            # 3. Cycle End (bit)
            cycle_end = points_by_tag["CYCLE_END"]
            self.assertEqual(cycle_end.value, 0)

            # 4. Shot timestamp
            self.assertEqual(points_by_tag["SHOT_YEAR"].value, 2026)
            self.assertEqual(points_by_tag["SHOT_MONTH"].value, 9)
            self.assertEqual(points_by_tag["SHOT_DAY"].value, 8)

            # 5. Cycle time (raw preserved)
            cycle_time = points_by_tag["CYCLE_TIME"]
            self.assertEqual(cycle_time.raw_value, 385)
            self.assertEqual(cycle_time.value, 385)
            self.assertEqual(cycle_time.unit, "sec")

            # 6. Temperatures
            self.assertEqual(points_by_tag["FIXED_DIE_TEMP_F1"].value, 185)
            self.assertEqual(points_by_tag["MOVING_DIE_TEMP_M1"].value, 175)

        finally:
            driver.disconnect()


if __name__ == "__main__":
    unittest.main()
