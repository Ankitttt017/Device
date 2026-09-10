"""
Mitsubishi SLMP (Seamless Message Protocol) 3E Binary Protocol Engine.
Strictly implements READ-ONLY frame construction and parsing.
Guarantees NO write commands can be framed or executed.
"""
import struct
from typing import Dict, List, Tuple


class SLMPProtocolError(Exception):
    """Raised when an SLMP protocol error occurs or end code is non-zero."""
    def __init__(self, message: str, end_code: int = 0):
        super().__init__(message)
        self.end_code = end_code


# Mitsubishi Device Codes for SLMP 3E Binary
DEVICE_CODES: Dict[str, int] = {
    "D": 0xA8,  # Data Register (word)
    "M": 0x90,  # Internal Relay (bit)
    "W": 0xB4,  # Link Register (word)
    "X": 0x9C,  # Input (bit)
    "Y": 0x9D,  # Output (bit)
    "L": 0x92,  # Latch Relay (bit)
    "B": 0xA0,  # Link Relay (bit)
    "F": 0x93,  # Annunciator (bit)
    "R": 0xAF,  # File Register (word)
    "ZR": 0xB0  # File Register (word)
}

# Subcommands
SUBCOMMAND_WORD_UNIT = 0x0000
SUBCOMMAND_BIT_UNIT = 0x0001

# SLMP 3E Header Constants
SUBHEADER_3E_REQUEST = 0x0050
SUBHEADER_3E_RESPONSE = 0x00D0
DEFAULT_NETWORK_NO = 0x00
DEFAULT_STATION_NO = 0xFF
DEFAULT_TARGET_MODULE_IO = 0x03FF  # Own station
DEFAULT_MULTIDROP_STATION = 0x00

# SLMP Read Command
SLMP_CMD_READ = 0x0401


def build_read_words_frame(
    device_type: str,
    head_device_number: int,
    points: int,
    monitoring_timer_250ms: int = 16,
    network_no: int = DEFAULT_NETWORK_NO,
    station_no: int = DEFAULT_STATION_NO,
    module_io: int = DEFAULT_TARGET_MODULE_IO,
    multidrop: int = DEFAULT_MULTIDROP_STATION
) -> bytes:
    """
    Constructs an SLMP 3E Binary request frame for batch read in WORD units.
    Command: 0x0401 (Read), Subcommand: 0x0000 (Word units).
    """
    dev_type = device_type.upper()
    if dev_type not in DEVICE_CODES:
        raise ValueError(f"Unknown SLMP device type: {device_type}")
    if points < 1 or points > 960:
        raise ValueError(f"Invalid read word points: {points}. Allowed range 1..960.")

    device_code = DEVICE_CODES[dev_type]

    # Request data:
    # Monitoring timer: 2 bytes
    # Command: 2 bytes (0x0401)
    # Subcommand: 2 bytes (0x0000)
    # Head device number: 3 bytes (24-bit unsigned little-endian)
    # Device code: 1 byte
    # Device points: 2 bytes (16-bit unsigned little-endian)
    dev_num_bytes = bytes([
        head_device_number & 0xFF,
        (head_device_number >> 8) & 0xFF,
        (head_device_number >> 16) & 0xFF
    ])

    request_data = (
        struct.pack("<H", monitoring_timer_250ms)
        + struct.pack("<H", SLMP_CMD_READ)
        + struct.pack("<H", SUBCOMMAND_WORD_UNIT)
        + dev_num_bytes
        + bytes([device_code])
        + struct.pack("<H", points)
    )

    request_data_len = len(request_data)

    header = (
        struct.pack("<H", SUBHEADER_3E_REQUEST)
        + bytes([network_no, station_no])
        + struct.pack("<H", module_io)
        + bytes([multidrop])
        + struct.pack("<H", request_data_len)
    )

    return header + request_data


def build_read_bits_frame(
    device_type: str,
    head_device_number: int,
    points: int,
    monitoring_timer_250ms: int = 16,
    network_no: int = DEFAULT_NETWORK_NO,
    station_no: int = DEFAULT_STATION_NO,
    module_io: int = DEFAULT_TARGET_MODULE_IO,
    multidrop: int = DEFAULT_MULTIDROP_STATION
) -> bytes:
    """
    Constructs an SLMP 3E Binary request frame for batch read in BIT units.
    Command: 0x0401 (Read), Subcommand: 0x0001 (Bit units).
    """
    dev_type = device_type.upper()
    if dev_type not in DEVICE_CODES:
        raise ValueError(f"Unknown SLMP device type: {device_type}")
    if points < 1 or points > 1792:
        raise ValueError(f"Invalid read bit points: {points}. Allowed range 1..1792.")

    device_code = DEVICE_CODES[dev_type]

    dev_num_bytes = bytes([
        head_device_number & 0xFF,
        (head_device_number >> 8) & 0xFF,
        (head_device_number >> 16) & 0xFF
    ])

    request_data = (
        struct.pack("<H", monitoring_timer_250ms)
        + struct.pack("<H", SLMP_CMD_READ)
        + struct.pack("<H", SUBCOMMAND_BIT_UNIT)
        + dev_num_bytes
        + bytes([device_code])
        + struct.pack("<H", points)
    )

    request_data_len = len(request_data)

    header = (
        struct.pack("<H", SUBHEADER_3E_REQUEST)
        + bytes([network_no, station_no])
        + struct.pack("<H", module_io)
        + bytes([multidrop])
        + struct.pack("<H", request_data_len)
    )

    return header + request_data


def parse_response_frame(response_bytes: bytes) -> bytes:
    """
    Validates and extracts data payload from an SLMP 3E Binary response frame.
    Raises SLMPProtocolError on malformed packet or non-zero End Code.
    """
    # Header: 2 (Subheader) + 1 (Net) + 1 (Station) + 2 (Module IO) + 1 (Multidrop) + 2 (Length) = 9 bytes
    # Data following length: 2 (End Code) + Payload
    # Minimum valid response size is 11 bytes.
    if len(response_bytes) < 11:
        raise SLMPProtocolError(
            f"Response frame too short ({len(response_bytes)} bytes, expected >= 11 bytes)"
        )

    subheader = struct.unpack("<H", response_bytes[:2])[0]
    if subheader != SUBHEADER_3E_RESPONSE:
        raise SLMPProtocolError(
            f"Invalid SLMP response subheader: 0x{subheader:04X}, expected 0x{SUBHEADER_3E_RESPONSE:04X}"
        )

    data_len = struct.unpack("<H", response_bytes[7:9])[0]
    expected_total_len = 9 + data_len
    if len(response_bytes) < expected_total_len:
        raise SLMPProtocolError(
            f"Incomplete SLMP response frame: received {len(response_bytes)} bytes, expected {expected_total_len}"
        )

    end_code = struct.unpack("<H", response_bytes[9:11])[0]
    if end_code != 0:
        error_detail = response_bytes[11:expected_total_len].hex()
        raise SLMPProtocolError(
            f"SLMP Error Response with End Code: 0x{end_code:04X}. Details: {error_detail}",
            end_code=end_code
        )

    # Return pure data payload
    return response_bytes[11:expected_total_len]


def parse_words_response(payload: bytes, expected_points: int) -> List[int]:
    """
    Parses word data payload into a list of 16-bit unsigned integers.
    """
    expected_bytes = expected_points * 2
    if len(payload) < expected_bytes:
        raise SLMPProtocolError(
            f"Word payload length mismatch: got {len(payload)} bytes, expected {expected_bytes} for {expected_points} words."
        )

    words: List[int] = []
    for i in range(0, expected_bytes, 2):
        word_val = struct.unpack("<H", payload[i:i+2])[0]
        words.append(word_val)
    return words


def parse_bits_response(payload: bytes, expected_points: int) -> List[int]:
    """
    Parses bit unit response payload into a list of 0/1 integers.
    In SLMP 3E binary bit read (0401/0001), each byte packs 2 points:
    - High 4 bits: Point N (1 = ON, 0 = OFF)
    - Low 4 bits: Point N+1 (1 = ON, 0 = OFF)
    """
    bits: List[int] = []
    for byte_val in payload:
        # First bit (high nibble)
        if len(bits) < expected_points:
            bits.append(1 if ((byte_val >> 4) & 0x0F) != 0 else 0)
        # Second bit (low nibble)
        if len(bits) < expected_points:
            bits.append(1 if (byte_val & 0x0F) != 0 else 0)
    return bits
