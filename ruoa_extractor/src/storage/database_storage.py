from typing import List, Optional
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from ruoa_extractor.src.storage.abstract_storage import AbstractRedditStorage
from ruoa_extractor.src.core.models import RedditPost, RedditComment
from ruoa_extractor.src.core.database import DatabaseManager


class DatabaseRedditStorage(AbstractRedditStorage):
    """Concrete implementation of Reddit storage using database"""

    def __init__(self, database_manager: DatabaseManager):
        self.db_manager = database_manager

    def save_post(self, post: RedditPost) -> bool:
        """Save a single Reddit post"""
        try:
            with self.db_manager.get_session() as session:
                session.merge(post)
                return True
        except Exception as e:
            print(f"Error saving post {post.id}: {e}")
            return False

    def save_posts(self, posts: List[RedditPost]) -> int:
        """Save multiple Reddit posts, return count of saved posts"""
        saved_count = 0

        with self.db_manager.get_session() as session:
            for post in posts:
                try:
                    session.merge(post)
                    saved_count += 1
                except Exception as e:
                    print(f"Error saving post {post.id}: {e}")
                    session.rollback()

        return saved_count

    def save_comment(self, comment: RedditComment) -> bool:
        """Save a single Reddit comment"""
        try:
            with self.db_manager.get_session() as session:
                session.merge(comment)
                return True
        except Exception as e:
            print(f"Error saving comment {comment.id}: {e}")
            return False

    def save_comments(self, comments: List[RedditComment]) -> int:
        """Save multiple Reddit comments, return count of saved comments"""
        saved_count = 0

        with self.db_manager.get_session() as session:
            for comment in comments:
                try:
                    session.merge(comment)
                    saved_count += 1
                except Exception as e:
                    print(f"Error saving comment {comment.id}: {e}")
                    session.rollback()

        return saved_count

    def post_exists(self, post_id: str) -> bool:
        """Check if a post already exists in storage"""
        with self.db_manager.get_session() as session:
            post = session.query(RedditPost).filter_by(id=post_id).first()
            return post is not None

    def comment_exists(self, comment_id: str) -> bool:
        """Check if a comment already exists in storage"""
        with self.db_manager.get_session() as session:
            comment = session.query(RedditComment).filter_by(id=comment_id).first()
            return comment is not None

    def get_latest_post_timestamp(self, subreddit: str) -> Optional[float]:
        """Get timestamp of the most recent post for incremental extraction"""
        with self.db_manager.get_session() as session:
            latest_post = (session.query(RedditPost)
                           .filter_by(subreddit=subreddit)
                           .order_by(RedditPost.created_utc.desc())
                           .first())

            if latest_post and latest_post.created_utc:
                return latest_post.created_utc.timestamp()
            return None

    def get_post_count(self, subreddit: str) -> int:
        """Get total count of posts for a subreddit"""
        with self.db_manager.get_session() as session:
            return session.query(func.count(RedditPost.id)).filter_by(subreddit=subreddit).scalar()

    def get_comment_count(self, subreddit: str) -> int:
        """Get total count of comments for a subreddit"""
        with self.db_manager.get_session() as session:
            return (session.query(func.count(RedditComment.id))
                    .join(RedditPost)
                    .filter(RedditPost.subreddit == subreddit)
                    .scalar())


if __name__ == "__main__":
    from ruoa_extractor.src.config.config import get_database_url

    print("Testing DatabaseRedditStorage...")

    db_manager = DatabaseManager(get_database_url(use_test_db=False))
    db_manager.create_tables()

    storage = DatabaseRedditStorage(db_manager)

    test_post = RedditPost(
        id="storage_test_1233",
        title="Storage Test Post",
        author="storage_test_user",
        subreddit="universityofauckland",
        score=10
    )

    test_comment = RedditComment(
        id="comment_test_1233",
        post_id="storage_test_123",
        body="Test comment for storage",
        author="comment_test_user",
        score=5
    )

    # Test saving
    post_saved = storage.save_post(test_post)
    comment_saved = storage.save_comment(test_comment)

    # Test existence checks
    post_exists = storage.post_exists("storage_test_123")
    comment_exists = storage.comment_exists("comment_test_123")

    # Test counts
    post_count = storage.get_post_count("universityofauckland")
    comment_count = storage.get_comment_count("universityofauckland")


    print(f"✅ Post saved: {post_saved}")
    print(f"✅ Comment saved: {comment_saved}")
    print(f"✅ Post exists: {post_exists}")
    print(f"✅ Comment exists: {comment_exists}")
    print(f"✅ Total posts: {post_count}")
    print(f"✅ Total comments: {comment_count}")
    print(f"Database URL: {get_database_url(use_test_db=False)}")