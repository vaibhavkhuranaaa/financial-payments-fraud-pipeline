# Developer Setup (macOS)

## Prerequisites
```bash
brew install colima docker docker-compose temurin uv
colima start --cpu 4 --memory 8        # Docker runtime (no Docker Desktop needed)
```

## Python environment
System Python 3.14 is too new for PySpark 3.5 — use a pinned 3.11 venv:
```bash
uv venv --python 3.11 .venv
source .venv/bin/activate
uv pip install -r requirements.txt -r requirements-dev.txt
```

## Data
```bash
python scripts/get_data.py            # downloads full TabFormer (~2GB) to data/raw/, out of git
python scripts/get_data.py --sample   # carves data/sample/transactions_sample.csv (~100k rows, committed)
```

## Verify everything
```bash
make check    # ruff + pytest + dbt build + terraform validate + compose config
```

## Config
Copy `.env.example` to `.env` and fill in values. Never commit `.env`.
