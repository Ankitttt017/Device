# QUAD Gateway — Industrial Edge Gateway
**Machine:** UBE 850 T - 02  
**Protocol:** Mitsubishi SLMP (Seamless Message Protocol) 3E Binary  
**Mode:** READ-ONLY

---

## 1. Project Overview
The **QUAD Gateway** is an industrial edge data acquisition system designed to reliably acquire telemetry from Mitsubishi PLCs and normalize it for downstream storage and MQTT synchronization.

### Safety Guarantee
- **Strict Read-Only Enforcement**: The gateway implements only SLMP Read (`0x0401`) commands. No write methods, commands, or execution paths exist in this codebase.
- **Unapproved Memory Protection**: The batch planner only combines strictly consecutive registers. It never reads arbitrary spans or skips across unconfigured registers.

---

## 2. Directory Structure

```text
Device/
├── quad_gateway/
│   ├── main.py                    # Gateway runtime entry point (Phase 1 & 2)
│   ├── validate_plc.py            # Real PLC connectivity & initial register validation
│   ├── config/
│   │   ├── config_loader.py       # Configuration parser & validator
│   │   └── machine.json           # Machine, storage & register definitions
│   ├── drivers/
│   │   └── slmp/
│   │       ├── slmp_protocol.py   # SLMP 3E Binary framing & parsing (Read-Only)
│   │       ├── slmp_driver.py     # Socket management & read operations
│   │       └── decoder.py         # Word, Bit, String & Scaled decoders
│   ├── acquisition/
│   │   └── collector.py           # Acquisition cycle & batch planning
│   ├── models/
│   │   └── telemetry.py           # Normalized telemetry data structures
│   ├── storage/                   # [Phase 2] SQLite persistent store & sync queue
│   │   ├── database.py            # SQLite StorageManager & transactions
│   │   └── schema.sql             # machines, tags, events, sync_queue schema
│   ├── mqtt/                      # [Phase 3 Reserved] MQTT publisher
│   ├── sync/                      # [Phase 5 Reserved] Backlog synchronization
│   └── utils/
│       └── logger.py              # Industrial logging
├── tests/
│   ├── mock_plc.py                # In-memory Mock Mitsubishi SLMP 3E Server
│   ├── test_slmp.py               # Protocol, driver, decoder & E2E tests
│   ├── test_config.py             # Configuration & batch planner tests
│   ├── test_validation.py         # Real/Mock PLC register validation tests
│   ├── test_storage.py            # [Phase 2] SQLite storage, schema, transaction tests
│   ├── test_mqtt.py               # [Phase 3 Reserved]
│   └── test_sync.py               # [Phase 5 Reserved]
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 3. Register Mapping (UBE 850 T - 02)

| Category | Count | Registers |
| :--- | :--- | :--- |
| **String / ASCII** | 1 | `Part Name` (D100-D110, 11 words / 22 bytes) |
| **INT16** | 14 | `Shot Year` (D2100), `Shot Month` (D2101), `Shot Day` (D2102), `Shot Hour` (D2103), `Shot Minute` (D2104), `Shot Second` (D2105), `SHOT NO.` (D1120), `HIGH SHOT COUNT` (D947), `NG COUNTER` (D955), `Cycle Start` (M840), `Cycle End` (M4598), `AVERAGE DIE CLAMP TONNAGE COUNT` (D7472), `Time for stroke(ms)` (D10470), `Shot Status` (D1301) |
| **Decimal / Scaled D** | 45 | `CYCLE TIME` (D1127), `DIE-CLOSE CORE IN TIME` (D1128), `POURING TIME` (D1129), `SHOT FWD TIME` (D1130), `DIE OPEN CORE OUT TIME` (D1132), `EJECTOR TIME` (D1133), `EXTRACT TIME` (D1134), `SPRAY TIME` (D1135), `CURING TIME` (D1137), `V1`..`V4` (D6900..D6906), `ACCEL. POINT` (D6908), `DEACEL. POINT` (D6910), `METAL PRESS.` (D6912), `INTEN. TIME` (D6914), `BISCUIT THICKNESS` (D6916), `CLAMP TONNAGE` (D6918..D6926), `VACUUM PRESSURE` (D6928), `COOLING WATER FLOW` (D6930..D6932), `FURNACE TEMP` (D6934), `CLAMP FORCE / TONNAGE` (D1044..D1045), `SHOT ACC / INTEN ACC` (D1700..D1701), `JET COOLING` (D6954), `DIE TEMPS` (D1400..D1404), `FLOWS` (D1410..D1415), `VACUUM` (D1416), `STROKE` (D10356) |

---

## 4. Running Tests

Run the comprehensive unit and integration test suite:
```powershell
& "C:\Users\ankitkumar2\AppData\Local\Python\pythoncore-3.14-64\python.exe" -m unittest discover -s tests -v
```

---

## 5. Real PLC Validation & Gateway Execution

### A. Real Mitsubishi PLC Validation:
Validates network connectivity and the Phase 1 priority register set against the physical machine PLC (`192.168.117.201:1027`):
```powershell
python -m quad_gateway.validate_plc
```
*(Or via main: `python -m quad_gateway.main --validate`)*

To test offline against the local Mock PLC server:
```powershell
python -m quad_gateway.validate_plc --mock
```

### B. Production Execution (Live PLC -> SQLite -> MQTT):
```powershell
python -m quad_gateway.main
```
Or for a single cycle:
```powershell
python -m quad_gateway.main --once
```

### C. Offline Simulation Mode (Mock PLC -> SQLite -> Fake MQTT):
```powershell
python -m quad_gateway.main --mock --fake-mqtt
```
Or for a single cycle:
```powershell
python -m quad_gateway.main --mock --fake-mqtt --once
```

### D. Offline Simulation with Real Broker (Mock PLC -> SQLite -> Local Broker):
```powershell
python -m quad_gateway.main --mock
```
