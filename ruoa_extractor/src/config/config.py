import os

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


def get_database_url(use_test_db: bool = False) -> str:
    db_settings = DatabaseSettings()
    return db_settings.test_url if use_test_db else db_settings.url


if __name__ == "__main__":
    db_settings = DatabaseSettings()
    print(f"Database URL: {db_settings.url}")
    print(f"Test Database URL: {db_settings.test_url}")