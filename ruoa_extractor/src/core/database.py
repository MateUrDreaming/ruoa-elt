from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from ruoa_extractor.src.core.models import Base


class DatabaseManager:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def create_tables(self) -> None:
        Base.metadata.create_all(bind=self.engine)

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


if __name__ == "__main__":
    db_manager = DatabaseManager("sqlite:///test_reddit.db")
    db_manager.create_tables()

    with db_manager.get_session() as session:
        from ruoa_extractor.src.core.models import RedditPost

        post = RedditPost(
            id="db_test_123",
            title="Database Test Post",
            author="test_user",
            subreddit="universityofauckland"
        )

        session.add(post)
        session.flush()

        retrieved = session.query(RedditPost).filter_by(id="db_test_123").first()
        print(f"✅ Database test successful: {retrieved}")