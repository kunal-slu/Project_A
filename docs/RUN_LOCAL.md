# Running Locally

## Setup

1. Create virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -e .[test]
```

3. Set environment:
```bash
export ENV=local
```

## Running the Pipeline

### Quick Start
```bash
./scripts/run_local_batch.sh
```

### Manual Run
```bash
export PYTHONPATH="$(pwd)/src:${PYTHONPATH:-}"
mkdir -p /tmp/lakehouse /tmp/lakehouse/_chk
python -m pyspark_interview_project --env local
```

## Expected Outputs

- `/tmp/lakehouse/bronze/` - Raw data
- `/tmp/lakehouse/silver/` - Cleaned data  
- `/tmp/lakehouse/gold/` - Business data
- `/tmp/lakehouse/_artifacts/` - Pipeline artifacts

## Development

- Use `docker-compose up` for MinIO (S3) and Azurite (ADLS) emulation
- Run tests: `pytest -q`
- Lint code: `ruff check .`
- Format code: `black .`









