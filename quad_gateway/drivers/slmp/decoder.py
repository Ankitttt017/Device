"""
Decoders for Mitsubishi SLMP register data.
Converts raw binary responses and 16-bit words into engineering values.
Strictly preserves raw PLC values and provides configurable decoding.
"""
import struct
from typing import Any, Optional, Tuple


def decode_int16(raw_bytes: bytes) -> int:
    """
    Decodes 2 bytes as signed 16-bit integer (little-endian).
    """
    if len(raw_bytes) < 2:
        raise ValueError(f"decode_int16 requires 2 bytes, got {len(raw_bytes)}")
    return struct.unpack("<h", raw_bytes[:2])[0]


def decode_uint16(raw_bytes: bytes) -> int:
    """
    Decodes 2 bytes as unsigned 16-bit integer (little-endian).
    """
    if len(raw_bytes) < 2:
        raise ValueError(f"decode_uint16 requires 2 bytes, got {len(raw_bytes)}")
    return struct.unpack("<H", raw_bytes[:2])[0]


def decode_scaled_decimal(raw_int16: int, scale_factor: Optional[float] = None) -> Tuple[int, Any]:
    """
    Decodes a scaled register value without guessing.
    Returns (raw_value, scaled_value).
    If scale_factor is None or 1.0, scaled_value equals raw_int16.
    """
    if scale_factor is None or scale_factor == 1.0:
        return raw_int16, raw_int16
    return raw_int16, round(raw_int16 * scale_factor, 6)


def decode_string(
    raw_bytes: bytes,
    byte_order: str = "little",
    encoding: str = "ascii",
    strip_null: bool = True
) -> str:
    """
    Decodes raw bytes (e.g. from D100-D110) into a string.
    
    In Mitsubishi PLCs, each 16-bit word holds 2 ASCII characters.
    - 'little' byte order (default): Low byte is Char 1, High byte is Char 2.
    - 'big' byte order: High byte is Char 1, Low byte is Char 2.
    """
    if byte_order == "big":
        # Swap adjacent bytes in each 16-bit word
        swapped = bytearray(len(raw_bytes))
        for i in range(0, len(raw_bytes) - 1, 2):
            swapped[i] = raw_bytes[i + 1]
            swapped[i + 1] = raw_bytes[i]
        if len(raw_bytes) % 2 == 1:
            swapped[-1] = raw_bytes[-1]
        decoded = bytes(swapped).decode(encoding, errors="replace")
    else:
        # Standard SLMP byte sequence (word low-byte first)
        decoded = raw_bytes.decode(encoding, errors="replace")

    if strip_null:
        # Strip trailing null characters and whitespace
        null_idx = decoded.find("\x00")
        if null_idx != -1:
            decoded = decoded[:null_idx]
        decoded = decoded.strip()

    return decoded


def decode_bit(bit_val: Any) -> int:
    """
    Normalizes a bit value to 0 or 1.
    """
    return 1 if bool(bit_val) else 0
