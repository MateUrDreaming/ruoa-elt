from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from ruoa_extractor.src.config import config
from ruoa_extractor.src.config.config import get_database_url
from ruoa_extractor.src.core.models import Base, RedditPost
from sqlalchemy import inspect

class DatabaseManager:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url, future=True, echo=True)
        self.SessionLocal = sessionmaker(bind=self.engine)


    def create_tables(self) -> None:
        # Create tables from metadata
        Base.metadata.create_all(bind=self.engine)
        created_tables = [table.name for table in Base.metadata.sorted_tables]
        print(f"✅ Tables created (or already exist): {created_tables}")

        # Inspect current tables in the database
        inspector = inspect(self.engine)
        current_tables = inspector.get_table_names()
        print(f"📋 Current tables in the database: {current_tables}")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()


if __name__ == "__main__":
    db_manager = DatabaseManager(get_database_url())
    with db_manager.get_session() as session:
        all_posts = session.query(RedditPost).all()

        if all_posts:
            print(f"✅ Successfully retrieved {len(all_posts)} posts:")
            for post in all_posts:
                print(f"- Post ID: {post.id}, Title: {post.title}")
        else:
            print("❌ No posts were found.")
