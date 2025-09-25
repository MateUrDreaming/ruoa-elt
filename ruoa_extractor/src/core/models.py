from datetime import datetime
from typing import Optional, List

from sqlalchemy import DateTime, Integer, String, Text, Boolean, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


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

    comments: Mapped[List["RedditComment"]] = relationship(back_populates="post")

    def __repr__(self) -> str:
        return f"<RedditPost(id='{self.id}', title='{self.title[:50]}...', author='{self.author}')>"


class RedditComment(Base):
    __tablename__ = "raw_reddit_comments"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    post_id: Mapped[str] = mapped_column(String, ForeignKey("raw_reddit_posts.id"))
    parent_id: Mapped[Optional[str]] = mapped_column(String)
    body: Mapped[Optional[str]] = mapped_column(Text)
    author: Mapped[Optional[str]] = mapped_column(String)
    created_utc: Mapped[Optional[datetime]] = mapped_column(DateTime)
    score: Mapped[Optional[int]] = mapped_column(Integer)
    is_submitter: Mapped[Optional[bool]] = mapped_column(Boolean)
    permalink: Mapped[Optional[str]] = mapped_column(String)
    extraction_timestamp: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )

    post: Mapped["RedditPost"] = relationship(back_populates="comments")

    def __repr__(self) -> str:
        body_preview = self.body[:50] + "..." if self.body and len(self.body) > 50 else self.body
        return f"<RedditComment(id='{self.id}', author='{self.author}', body='{body_preview}')>"


if __name__ == "__main__":
    print("Testing Reddit models...")

    post = RedditPost(
        id="test123",
        title="Test Post from r/universityofauckland",
        author="test_user",
        subreddit="universityofauckland",
        score=5
    )

    comment = RedditComment(
        id="comment123",
        post_id="test123",
        body="This is a test comment",
        author="commenter_user",
        score=3
    )

    print(f"Created post: {post}")
    print(f"Created comment: {comment}")
    print("✅ Both Reddit models working!")