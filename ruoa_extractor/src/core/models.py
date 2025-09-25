from datetime import datetime
from typing import Optional, List
from decimal import Decimal

from sqlalchemy import DateTime, Integer, String, Text, Boolean, ForeignKey, Numeric
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class RedditPost(Base):
    __tablename__ = "raw_reddit_posts"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    selftext: Mapped[Optional[str]] = mapped_column(Text)
    author: Mapped[Optional[str]] = mapped_column(String)
    created_utc: Mapped[Optional[datetime]] = mapped_column(DateTime)
    score: Mapped[Optional[int]] = mapped_column(Integer)
    num_comments: Mapped[Optional[int]] = mapped_column(Integer)
    upvote_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3))
    url: Mapped[Optional[str]] = mapped_column(Text)
    subreddit: Mapped[Optional[str]] = mapped_column(String)
    flair_text: Mapped[Optional[str]] = mapped_column(String)
    flair_css_class: Mapped[Optional[str]] = mapped_column(String)
    is_video: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_self: Mapped[Optional[bool]] = mapped_column(Boolean)
    permalink: Mapped[Optional[str]] = mapped_column(String)
    post_hint: Mapped[Optional[str]] = mapped_column(String)
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
    print("Testing complete Reddit models...")

    post = RedditPost(
        id="test123",
        title="Complete Test Post from r/universityofauckland",
        selftext="This is the complete post content with all fields",
        author="test_user",
        subreddit="universityofauckland",
        score=15,
        num_comments=5,
        upvote_ratio=Decimal("0.87"),
        url="https://reddit.com/r/universityofauckland/test123",
        flair_text="Discussion",
        flair_css_class="discussion",
        is_video=False,
        is_self=True,
        permalink="/r/universityofauckland/comments/test123/complete_test_post/",
        post_hint="self"
    )

    comment = RedditComment(
        id="comment123",
        post_id="test123",
        parent_id="test123",
        body="This is a comprehensive test comment",
        author="commenter_user",
        score=3,
        is_submitter=False,
        permalink="/r/universityofauckland/comments/test123/complete_test_post/comment123/"
    )

    print(f"Created complete post: {post}")
    print(f"Created complete comment: {comment}")
    print("✅ Complete Reddit models working with all fields!")