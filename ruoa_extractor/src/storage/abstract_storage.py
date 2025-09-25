from abc import ABC, abstractmethod
from typing import List, Optional

from ruoa_extractor.src.core.models import RedditPost, RedditComment


class AbstractRedditStorage(ABC):
    """Abstract base class for Reddit data storage"""

    @abstractmethod
    def save_post(self, post: RedditPost) -> bool:
        """Save a single Reddit post"""
        pass

    @abstractmethod
    def save_posts(self, posts: List[RedditPost]) -> int:
        """Save multiple Reddit posts, return count of saved posts"""
        pass

    @abstractmethod
    def save_comment(self, comment: RedditComment) -> bool:
        """Save a single Reddit comment"""
        pass

    @abstractmethod
    def save_comments(self, comments: List[RedditComment]) -> int:
        """Save multiple Reddit comments, return count of saved comments"""
        pass

    @abstractmethod
    def post_exists(self, post_id: str) -> bool:
        """Check if a post already exists in storage"""
        pass

    @abstractmethod
    def comment_exists(self, comment_id: str) -> bool:
        """Check if a comment already exists in storage"""
        pass

    @abstractmethod
    def get_latest_post_timestamp(self, subreddit: str) -> Optional[float]:
        """Get timestamp of the most recent post for incremental extraction"""
        pass


if __name__ == "__main__":
    print("AbstractRedditStorage created successfully!")
    print("This is an abstract class - implement all storage methods.")