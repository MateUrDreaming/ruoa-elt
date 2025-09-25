from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RedditPost(Base):
    __tablename__ = "raw_reddit_posts"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    author: Mapped[Optional[str]] = mapped_column(String)
    created_utc: Mapped[Optional[datetime]] = mapped_column(DateTime)
    score: Mapped[Optional[int]] = mapped_column(Integer)
    subreddit: Mapped[Optional[str]] = mapped_column(String)
    extraction_timestamp: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )

    def __repr__(self) -> str:
        return f"<RedditPost(id='{self.id}', title='{self.title[:50]}...', author='{self.author}')>"


if __name__ == "__main__":
    print("Testing RedditPost model...")

    post = RedditPost(
        id="test123",
        title="Test Post from r/universityofauckland",
        author="test_user",
        subreddit="universityofauckland",
        score=5
    )

    print(f"Created: {post}")
    print("✅ RedditPost model working!")