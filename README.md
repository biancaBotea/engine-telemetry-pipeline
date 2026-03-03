# Engine Telemetry Pipeline
A robust data engineering pipeline for ingesting, validating, and analyzing synthetic combustion engine telemetry using Apache Airflow, Python, and SQLite.


## Phase 1: Engine Telemetry Generator

The first component is a Python-based simulation engine. It is designed to model realistic internal combustion engine behavior while intentionally introducing data quality issues to test pipeline resilience.

### Telemetry Specification
Each engine produces a CSV file containing 90 minutes of data with the following schema:

| Field | Type | Description |
| :--- | :--- | :--- |
| `timestamp` | ISO-8601 | UTC timestamp of the sensor reading. |
| `engine_id` | String | Unique identifier for the asset (e.g., `ENG-001`). |
| `rpm` | Float | Engine Revolutions Per Minute. |
| `temp` | Float | Engine temperature in Celsius (°C). |
| `oil_pressure`| Float | Lubrication system pressure in Bar. |
| `fuel_cons` | Float | Rate of fuel consumption. |
| `status` | String | Logic-based health state (`running`, `warning`, `error`). |

### Chaos Engineering
The generator injects "Realistic Anomalies":

* **Sentinel Values:** `RPM` may drop to `-999.0` or `Temp` may spike to `999.9` to simulate hardware sensor failure.
* **Null Values:** `oil_pressure` occasionally reports as `None` (empty in CSV) to simulate intermittent signal loss.
* **Self-Healing Physics:** The simulation recovers from anomalies in the subsequent timestep, allowing for the testing of point-anomaly filtering vs. persistent failure detection.

### How to Run
1. Ensure you have Python 3.x installed.
2. Run the generator script:
   ```bash
   python generator.py