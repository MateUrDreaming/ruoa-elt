import os
from dotenv import load_dotenv
from pathlib import Path

current_file_path = Path(__file__).resolve()
dotenv_path = current_file_path.parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

class DatabaseSettings:
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.name = os.getenv("POSTGRES_DB", "ruoa")
        self.user = os.getenv("POSTGRES_USER", "postgres")
        self.password = os.getenv("POSTGRES_PASSWORD", "postgres")

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def test_url(self) -> str:
        return "sqlite:///test_reddit.db"


class RedditSettings:
    def __init__(self):
        self.client_id = os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        self.user_agent = os.getenv("REDDIT_USER_AGENT", "pk-uoa-etl/1.0")

    def is_configured(self) -> bool:
        """Check if required Reddit credentials are set (read-only access)"""
        return all([
            self.client_id,
            self.client_secret,
            self.user_agent
        ])


def get_database_url(use_test_db: bool = False) -> str:
    db_settings = DatabaseSettings()
    return db_settings.test_url if use_test_db else db_settings.url


def get_reddit_settings() -> RedditSettings:
    return RedditSettings()


if __name__ == "__main__":
    db_settings = DatabaseSettings()
    reddit_settings = RedditSettings()
    print(dotenv_path)
    print(f"Database URL: {db_settings.url}")
    print(f"Test Database URL: {db_settings.test_url}")
    print(f"Reddit configured: {reddit_settings.is_configured()}")
    print(f"Reddit user agent: {reddit_settings.user_agent}")
