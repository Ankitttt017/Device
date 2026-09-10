"""
Mock Mitsubishi SLMP 3E Binary PLC Server.
Used for offline unit and integration testing without requiring a physical machine.
Simulates memory registers for machine UBE 850 T - 02.
"""
import socket
import struct
import threading
from typing import Dict, Optional


class MockSLMPServer:
    """
    In-memory SLMP 3E Binary TCP server simulating a Mitsubishi Q/L series PLC.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.port = port
        self._server_socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # In-memory register maps
        self.d_registers: Dict[int, int] = {}  # Address -> 16-bit unsigned int
        self.m_bits: Dict[int, int] = {}  # Address -> 0 or 1

        self._init_default_data()

    def _init_default_data(self):
        """
        Populates registers with realistic UBE 850 T - 02 machine data.
        """
        # Part Name: D100-D110 ("PART-UBE-850T02\x00")
        part_name_ascii = "PART-UBE-850T02\x00"
        name_bytes = part_name_ascii.encode("ascii")
        # Pad to 22 bytes (11 words)
        name_bytes = name_bytes.ljust(22, b"\x00")
        for i in range(11):
            w_val = struct.unpack("<H", name_bytes[i*2:(i+1)*2])[0]
            self.d_registers[100 + i] = w_val

        # Shot timestamp: D2100-D2105
        self.d_registers[2100] = 2026
        self.d_registers[2101] = 9
        self.d_registers[2102] = 8
        self.d_registers[2103] = 15
        self.d_registers[2104] = 45
        self.d_registers[2105] = 12

        # Counters
        self.d_registers[1120] = 1042   # SHOT NO.
        self.d_registers[947] = 25000   # HIGH SHOT COUNT
        self.d_registers[955] = 3       # NG COUNTER
        self.d_registers[7472] = 850    # AVG DIE CLAMP TONNAGE
        self.d_registers[10470] = 320   # Time for stroke ms
        self.d_registers[1301] = 1      # Shot Status

        # Bit registers
        self.m_bits[840] = 1            # Cycle Start
        self.m_bits[4598] = 0           # Cycle End

        # Process Times (D1127-D1137)
        self.d_registers[1127] = 385    # CYCLE TIME (e.g. 38.5s)
        self.d_registers[1128] = 42     # DIE CLOSE
        self.d_registers[1129] = 28     # POURING
        self.d_registers[1130] = 18     # SHOT FWD
        self.d_registers[1132] = 55     # DIE OPEN
        self.d_registers[1133] = 12     # EJECTOR
        self.d_registers[1134] = 22     # EXTRACT
        self.d_registers[1135] = 34     # SPRAY
        self.d_registers[1137] = 75     # CURING

        # Pressures & Velocities (D6900-D6934)
        self.d_registers[6900] = 25     # V1
        self.d_registers[6902] = 120    # V2
        self.d_registers[6904] = 250    # V3
        self.d_registers[6906] = 310    # V4
        self.d_registers[6908] = 150    # ACCEL. POINT
        self.d_registers[6910] = 420    # DEACEL. POINT
        self.d_registers[6912] = 680    # METAL PRESS
        self.d_registers[6914] = 45     # INTEN. TIME
        self.d_registers[6916] = 250    # BISCUIT THICKNESS
        self.d_registers[6918] = 98     # CLAMP TONNAGE HE.LOW %
        self.d_registers[6920] = 85     # CLAMP TONNAGE HE.LOW MN
        self.d_registers[6922] = 99     # OP.UP %
        self.d_registers[6924] = 97     # OP.LOW %
        self.d_registers[6926] = 99     # HE.UP %
        self.d_registers[6928] = 12     # VACUUM PRESSURE
        self.d_registers[6930] = 145    # WATER FLOW MOV
        self.d_registers[6932] = 142    # WATER FLOW STA
        self.d_registers[6934] = 685    # FURNACE TEMP

        # Clamp & Acc (D1044-D1701)
        self.d_registers[1044] = 98     # CLAMP FORCE %
        self.d_registers[1045] = 850    # CLAMP TONNAGE T
        self.d_registers[1700] = 140    # SHOT ACC PRESS
        self.d_registers[1701] = 145    # INTEN ACC PRESS
        self.d_registers[6954] = 8      # JET COOLING PRESS

        # Temps (D1400-D1404)
        self.d_registers[1400] = 185    # F-1
        self.d_registers[1401] = 190    # F-2
        self.d_registers[1402] = 175    # M-1
        self.d_registers[1403] = 180    # M-2
        self.d_registers[1404] = 160    # S-1

        # Flows (D1410-D1416)
        self.d_registers[1410] = 45     # FIX 1
        self.d_registers[1411] = 42     # FIX 2
        self.d_registers[1412] = 44     # FIX 3
        self.d_registers[1413] = 50     # MOV 1
        self.d_registers[1414] = 52     # MOV 2
        self.d_registers[1415] = 48     # MOV 3
        self.d_registers[1416] = 15     # Vacuum mmHg
        self.d_registers[10356] = 850   # Stroke mm

    def start(self):
        """
        Starts the mock TCP server in a background daemon thread.
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self.port = self._server_socket.getsockname()[1]
        self._server_socket.listen(5)
        self._running = True

        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """
        Stops the mock server and closes open sockets.
        """
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=1.0)

    def _accept_loop(self):
        while self._running:
            try:
                client_sock, _ = self._server_socket.accept()
                client_thread = threading.Thread(target=self._handle_client, args=(client_sock,), daemon=True)
                client_thread.start()
            except Exception:
                break

    def _handle_client(self, client_sock: socket.socket):
        with client_sock:
            while self._running:
                try:
                    # Read SLMP header (9 bytes)
                    header = self._recv_exact(client_sock, 9)
                    if not header:
                        break
                    data_len = struct.unpack("<H", header[7:9])[0]
                    body = self._recv_exact(client_sock, data_len)
                    if not body:
                        break

                    # Parse request
                    response = self._process_request(body)
                    client_sock.sendall(response)
                except Exception:
                    break

    def _recv_exact(self, sock: socket.socket, num_bytes: int) -> bytes:
        data = bytearray()
        while len(data) < num_bytes:
            chunk = sock.recv(num_bytes - len(data))
            if not chunk:
                return bytes(data)
            data.extend(chunk)
        return bytes(data)

    def _process_request(self, body: bytes) -> bytes:
        """
        Processes SLMP request data:
        Timer(2) + Cmd(2) + Subcmd(2) + DevNum(3) + DevCode(1) + Points(2)
        """
        if len(body) < 12:
            return self._build_error_response(0xC059)  # Format error

        cmd = struct.unpack("<H", body[2:4])[0]
        subcmd = struct.unpack("<H", body[4:6])[0]
        dev_num = body[6] | (body[7] << 8) | (body[8] << 16)
        dev_code = body[9]
        points = struct.unpack("<H", body[10:12])[0]

        if cmd != 0x0401:
            # Read command only!
            return self._build_error_response(0xC059)

        if subcmd == 0x0000:
            # Word unit read
            data_bytes = bytearray()
            for i in range(points):
                addr = dev_num + i
                val = self.d_registers.get(addr, 0)
                data_bytes.extend(struct.pack("<H", val))
            return self._build_success_response(bytes(data_bytes))

        elif subcmd == 0x0001:
            # Bit unit read
            # Pack bits: 2 points per byte (high nibble point 1, low nibble point 2)
            data_bytes = bytearray()
            for i in range(0, points, 2):
                p1 = self.m_bits.get(dev_num + i, 0)
                p2 = self.m_bits.get(dev_num + i + 1, 0) if (i + 1 < points) else 0
                byte_val = ((p1 & 0x01) << 4) | (p2 & 0x01)
                data_bytes.append(byte_val)
            return self._build_success_response(bytes(data_bytes))

        return self._build_error_response(0xC059)

    def _build_success_response(self, data_payload: bytes) -> bytes:
        end_code = 0x0000
        length = 2 + len(data_payload)  # End code (2) + payload
        header = (
            struct.pack("<H", 0x00D0)  # Subheader
            + bytes([0x00, 0xFF])      # Network, Station
            + struct.pack("<H", 0x03FF)# Module IO
            + bytes([0x00])            # Multidrop
            + struct.pack("<H", length)# Data length
            + struct.pack("<H", end_code)
        )
        return header + data_payload

    def _build_error_response(self, end_code: int) -> bytes:
        length = 2  # Only end code
        header = (
            struct.pack("<H", 0x00D0)
            + bytes([0x00, 0xFF])
            + struct.pack("<H", 0x03FF)
            + bytes([0x00])
            + struct.pack("<H", length)
            + struct.pack("<H", end_code)
        )
        return header
