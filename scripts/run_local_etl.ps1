$ErrorActionPreference = "Stop"

if (-not (Test-Path ".venv")) {
  python -m venv .venv
}

.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest
python -m telemetry_etl.transform data/sample/tesla_telemetry_sample.jsonl build/curated
