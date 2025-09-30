# RUOA Extractor

Reddit ETL Pipeline for r/universityofauckland

## Project Structure

```
ruoa-extractor/
├── src/
│   ├── __init__.py
│   ├── config/         # Environment variable setup
│   ├── core/           # Core models and database setup
│   ├── extractors/     # Reddit data extraction logic with PRAW
│   ├── pipeline/       # Extraction operations setup
│   └── storage/        # Database storage and operations
├── tests/              # Test suite
├── requirements.txt
└── README.md
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run tests:
```bash
python -m pytest tests/
```

## Current Status

- [x] Project structure
- [ ] Core models
- [ ] Reddit extractor
- [ ] Database storage
- [ ] Configuration
- [ ] Tests
