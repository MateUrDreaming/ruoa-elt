# RUOA Extractor

Reddit ETL Pipeline for r/universityofauckland

## Project Structure

```
ruoa-extractor/
├── src/
│   ├── __init__.py
│   ├── config/         # Configuration and environment variables
│   ├── core/           # Core models and database setup
│   ├── extractors/     # Reddit data extraction logic with PRAW
│   ├── pipeline/       # ETL pipeline orchestration
│   └── storage/        # Database storage operations
├── tests/              # Test suite (unit + integration)
│   ├── unit/
│   ├── integration/
│   ├── fixtures/
│   └── conftest.py
├── main.py             # Application entry point
├── requirements.txt
└── README.md
```

## Setup

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Configure environment variables:**
Create a `.env` file in the project root:
```env
# Database Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=ruoa
DB_HOST=localhost
DB_PORT=5432

# Reddit API Configuration
REDDIT_CLIENT_ID=your_client_id_here
REDDIT_CLIENT_SECRET=your_client_secret_here
REDDIT_USER_AGENT=pk-uoa-etl/1.0
```

3. **Set up database:**
```bash
# Make sure PostgreSQL is running, then run:
python -c "from ruoa_extractor.src.core.database import DatabaseManager; from ruoa_extractor.src.config.settings import get_database_url; db = DatabaseManager(get_database_url()); db.create_tables()"
```

## Running the Application

### Single Extraction
Extract posts and comments once:
```bash
python main.py extract
```

With custom parameters:
```bash
# Extract 50 posts from this week with max 20 comments per post
python main.py extract --posts 50 --filter week --comments 20

# Use different subreddit
python main.py extract --subreddit newzealand --posts 10
```

### Continuous Mode
Run extraction every N hours:
```bash
# Run every 12 hours (default)
python main.py continuous

# Run every 6 hours
python main.py continuous --interval 6
```

### View Statistics
```bash
python main.py stats
```

### Additional Options
```bash
# Use test database (SQLite)
python main.py extract --test

# Enable debug logging
python main.py extract --log-level DEBUG

# Show all options
python main.py --help
```

## Running Tests

### Run all tests
```bash
python -m pytest ruoa_extractor/tests/ -v
```

### Run unit tests only
```bash
python -m pytest ruoa_extractor/tests/unit/ -v
```

### Run integration tests only
```bash
python -m pytest ruoa_extractor/tests/integration/ -v
```

### Run specific test file
```bash
python -m pytest ruoa_extractor/tests/unit/test_models.py -v
```

### Run with coverage report
```bash
python -m pytest ruoa_extractor/tests/ -v --cov=src --cov-report=html
```

### Run tests matching pattern
```bash
python -m pytest ruoa_extractor/tests/ -k "test_post" -v
```

## Development

### Project Status

- [x] Project structure with modular design
- [x] SQLAlchemy 2.0 models with type hints
- [x] PRAW Reddit extractor with error handling
- [x] Database storage with upsert support
- [x] Configuration management with environment variables
- [x] Complete ETL pipeline orchestration
- [x] CLI application entry point
- [x] Comprehensive test suite (unit + integration)
- [x] Abstract base classes for extensibility
- [x] Logging and monitoring capabilities

### Key Features

- **Incremental Extraction**: Avoids duplicate data with existence checks
- **Batch Operations**: Efficient bulk inserts for large datasets
- **Error Recovery**: Graceful failure handling with detailed logging
- **Type Safety**: Full type hints throughout codebase
- **Extensible Design**: Abstract base classes for future implementations
- **Test Coverage**: 100+ unit and integration tests
- **CLI Interface**: User-friendly command-line operations
- **Continuous Mode**: Scheduled extraction for automated workflows

## Architecture

### Core Components

1. **Models** (`src/core/models.py`): SQLAlchemy 2.0 models for posts and comments
2. **Database** (`src/core/database.py`): Database connection and session management
3. **Extractor** (`src/extractors/praw_extractor.py`): Reddit API data extraction
4. **Storage** (`src/storage/database_storage.py`): Database persistence layer
5. **Pipeline** (`src/pipeline/reddit_etl.py`): ETL orchestration and workflow
6. **Config** (`src/config/settings.py`): Environment-based configuration

### Design Patterns

- **Abstract Base Classes**: Extensible interfaces for extractors and storage
- **Dependency Injection**: Loosely coupled components
- **Context Managers**: Proper resource management for database sessions
- **Factory Pattern**: Configuration and settings instantiation
- **Repository Pattern**: Data access abstraction

## Troubleshooting

### Database Connection Issues
```bash
# Test database connection
python debug_database.py

# Check database URL
python -c "from ruoa_extractor.src.config.settings import get_database_url; print(get_database_url())"
```

### Reddit API Issues
```bash
# Verify Reddit credentials
python -c "from ruoa_extractor.src.config.settings import get_reddit_settings; s = get_reddit_settings(); print(f'Configured: {s.is_configured()}')"
```

### Test Database
```bash
# Run with SQLite test database
python main.py extract --test
```

