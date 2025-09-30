from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

from ruoa_extractor.src.core.models import RedditPost, RedditComment


class AbstractRedditExtractor(ABC):
    """Abstract base class for Reddit data extractors"""

    def __init__(self, subreddit_name: str):
        self.subreddit_name = subreddit_name

    @abstractmethod
    def extract_posts(self, limit: int = 10, time_filter: str = "day") -> List[RedditPost]:
        """Extract Reddit posts from the subreddit"""
        pass

    @abstractmethod
    def extract_comments(self, post_id: str, limit: Optional[int] = None) -> List[RedditComment]:
        """Extract comments for a specific post"""
        pass

    @abstractmethod
    def extract_posts_with_comments(
            self,
            limit: int = 10,
            time_filter: str = "day",
            comment_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Extract posts along with their comments"""
        pass

    def _convert_timestamp(self, unix_timestamp: float) -> datetime:
        """Convert Unix timestamp to datetime object"""
        return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

    def _sanitize_text(self, text: Optional[str]) -> Optional[str]:
        """Clean and sanitize text content"""
        if not text:
            return None
        return text.strip().replace('\x00', '')


if __name__ == "__main__":
    print("AbstractRedditExtractor created successfully!")
    print("This is an abstract class - implement extract_posts and extract_comments methods.")