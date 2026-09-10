"""
Mitsubishi SLMP 3E Binary Driver for Industrial Edge Gateways.
Implements robust TCP communication with strict READ-ONLY enforcement.
All write methods are intentionally omitted to provide an architectural safety boundary.
"""
import logging
import math
import socket
import struct
import time
from typing import Any, Dict, List, Optional

from quad_gateway.drivers.slmp.slmp_protocol import (
    SLMPProtocolError,
    build_read_words_frame,
    build_read_bits_frame,
    parse_response_frame,
    parse_words_response,
    parse_bits_response,
    SUBHEADER_3E_RESPONSE
)


logger = logging.getLogger("quad_gateway.slmp_driver")


class SLMPDriverError(Exception):
    """Base exception for SLMP driver errors."""
    pass


class SLMPConnectionError(SLMPDriverError):
    """Raised when socket connection fails or disconnects."""
    pass


class SLMPTimeoutError(SLMPDriverError):
    """Raised when a socket timeout occurs."""
    pass


class SLMPDriver:
    """
    Industrial SLMP 3E Binary Client.
    Exclusively provides READ-ONLY access to Mitsubishi PLCs.
    """

    def __init__(
        self,
        host: str,
        port: int,
        timeout_seconds: float = 5.0,
        monitoring_timer_250ms: int = 16,
        read_only: bool = True
    ):
        if not read_only:
            raise SLMPDriverError("SAFETY VIOLATION: Driver must run with read_only=True.")

        self.host = host
        self.port = port
        self.timeout_seconds = timeout_seconds
        self.monitoring_timer_250ms = monitoring_timer_250ms
        self.read_only = read_only

        self._socket: Optional[socket.socket] = None
        self._is_connected = False

    @property
    def is_connected(self) -> bool:
        return self._is_connected and self._socket is not None

    def connect(self) -> None:
        """
        Establishes TCP connection to the PLC.
        """
        if self.is_connected:
            return

        logger.info(f"Connecting to Mitsubishi PLC at {self.host}:{self.port} (Timeout: {self.timeout_seconds}s)...")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout_seconds)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # Enable keepalive
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            sock.connect((self.host, self.port))
            self._socket = sock
            self._is_connected = True
            logger.info(f"Connected successfully to PLC at {self.host}:{self.port}")
        except socket.timeout as e:
            sock.close()
            self._cleanup_socket()
            logger.error(f"Connection timeout to PLC {self.host}:{self.port}: {e}")
            raise SLMPTimeoutError(f"Connection timeout to PLC {self.host}:{self.port}: {e}") from e
        except OSError as e:
            sock.close()
            self._cleanup_socket()
            logger.error(f"Failed to connect to PLC {self.host}:{self.port}: {e}")
            raise SLMPConnectionError(f"Failed to connect to PLC {self.host}:{self.port}: {e}") from e

    def disconnect(self) -> None:
        """
        Closes TCP connection to the PLC.
        """
        logger.info(f"Disconnecting from PLC {self.host}:{self.port}...")
        self._cleanup_socket()

    def _cleanup_socket(self) -> None:
        if self._socket:
            try:
                self._socket.close()
            except Exception as e:
                logger.debug(f"Error closing socket: {e}")
        self._socket = None
        self._is_connected = False

    def _send_and_receive(self, request_frame: bytes) -> bytes:
        """
        Sends request frame and receives complete SLMP response frame over TCP.
        Handles TCP streaming and reassembly.
        """
        if not self.is_connected or self._socket is None:
            self.connect()

        try:
            # Send request
            self._socket.sendall(request_frame)

            # Receive header: minimum 9 bytes to know response data length
            header_bytes = self._recv_exact(9)
            
            # Verify subheader
            subheader = struct.unpack("<H", header_bytes[:2])[0]
            if subheader != SUBHEADER_3E_RESPONSE:
                raise SLMPProtocolError(f"Invalid subheader in response: 0x{subheader:04X}")

            # Data length is at offset 7 (2 bytes little-endian)
            data_length = struct.unpack("<H", header_bytes[7:9])[0]
            
            # Receive remaining response body
            body_bytes = self._recv_exact(data_length)

            full_frame = header_bytes + body_bytes
            return parse_response_frame(full_frame)

        except socket.timeout as e:
            self._cleanup_socket()
            logger.error(f"Socket timeout during SLMP communication with {self.host}:{self.port}: {e}")
            raise SLMPTimeoutError(f"Timeout communicating with PLC: {e}") from e
        except (OSError, BrokenPipeError, ConnectionResetError) as e:
            self._cleanup_socket()
            logger.error(f"Connection error during SLMP communication with {self.host}:{self.port}: {e}")
            raise SLMPConnectionError(f"Connection error with PLC: {e}") from e

    def _recv_exact(self, num_bytes: int) -> bytes:
        """
        Receives exactly num_bytes from the TCP socket.
        """
        received = bytearray()
        while len(received) < num_bytes:
            chunk = self._socket.recv(num_bytes - len(received))
            if not chunk:
                raise SLMPConnectionError("PLC closed socket unexpectedly.")
            received.extend(chunk)
        return bytes(received)

    def read_words(self, device_type: str, head_device_number: int, points: int) -> List[int]:
        """
        Reads contiguous word devices (e.g. D1127, D1128...) in WORD units.
        Returns list of 16-bit unsigned integers.
        """
        frame = build_read_words_frame(
            device_type=device_type,
            head_device_number=head_device_number,
            points=points,
            monitoring_timer_250ms=self.monitoring_timer_250ms
        )
        payload = self._send_and_receive(frame)
        return parse_words_response(payload, points)

    def read_raw_bytes(self, device_type: str, head_device_number: int, points: int) -> bytes:
        """
        Reads contiguous word devices and returns raw binary bytes (for strings or custom decoding).
        """
        frame = build_read_words_frame(
            device_type=device_type,
            head_device_number=head_device_number,
            points=points,
            monitoring_timer_250ms=self.monitoring_timer_250ms
        )
        return self._send_and_receive(frame)

    def read_bits(self, device_type: str, head_device_number: int, points: int) -> List[int]:
        """
        Reads bit devices (e.g. M840, M4598) in BIT units.
        Returns list of 0/1 integers.
        """
        frame = build_read_bits_frame(
            device_type=device_type,
            head_device_number=head_device_number,
            points=points,
            monitoring_timer_250ms=self.monitoring_timer_250ms
        )
        payload = self._send_and_receive(frame)
        return parse_bits_response(payload, points)
