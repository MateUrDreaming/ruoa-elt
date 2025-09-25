import pytest
from abc import ABC
from datetime import datetime

from ruoa_extractor.src.extractors.abstract_extractor import AbstractRedditExtractor
from ruoa_extractor.src.storage.abstract_storage import AbstractRedditStorage
from ruoa_extractor.src.core.models import RedditPost, RedditComment


class TestAbstractRedditExtractor:
    """Test cases for AbstractRedditExtractor"""

    def test_abstract_extractor_is_abstract(self):
        """Test that AbstractRedditExtractor cannot be instantiated directly"""
        with pytest.raises(TypeError):
            AbstractRedditExtractor("test_subreddit")

    def test_abstract_extractor_inheritance(self):
        """Test that AbstractRedditExtractor inherits from ABC"""
        assert issubclass(AbstractRedditExtractor, ABC)

    def test_concrete_implementation_works(self):
        """Test that concrete implementation can be instantiated"""

        class ConcreteExtractor(AbstractRedditExtractor):
            def extract_posts(self, limit=10, time_filter="day"):
                return []

            def extract_comments(self, post_id, limit=None):
                return []

            def extract_posts_with_comments(self, limit=10, time_filter="day", comment_limit=None):
                return {"posts": [], "comments": []}

        extractor = ConcreteExtractor("test_subreddit")
        assert extractor.subreddit_name == "test_subreddit"

    def test_convert_timestamp_utility(self):
        """Test the _convert_timestamp utility method"""

        class ConcreteExtractor(AbstractRedditExtractor):
            def extract_posts(self, limit=10, time_filter="day"):
                return []

            def extract_comments(self, post_id, limit=None):
                return []

            def extract_posts_with_comments(self, limit=10, time_filter="day", comment_limit=None):
                return {"posts": [], "comments": []}

        extractor = ConcreteExtractor("test")

        # Test Unix timestamp conversion
        unix_timestamp = 1640995200.0  # 2022-01-01 00:00:00 UTC
        converted = extractor._convert_timestamp(unix_timestamp)

        assert isinstance(converted, datetime)
        assert converted.year == 2022
        assert converted.month == 1
        assert converted.day == 1

    def test_sanitize_text_utility(self):
        """Test the _sanitize_text utility method"""

        class ConcreteExtractor(AbstractRedditExtractor):
            def extract_posts(self, limit=10, time_filter="day"):
                return []

            def extract_comments(self, post_id, limit=None):
                return []

            def extract_posts_with_comments(self, limit=10, time_filter="day", comment_limit=None):
                return {"posts": [], "comments": []}

        extractor = ConcreteExtractor("test")

        # Test normal text
        assert extractor._sanitize_text("Hello World") == "Hello World"

        # Test text with whitespace
        assert extractor._sanitize_text("  Hello World  ") == "Hello World"

        # Test text with null bytes
        assert extractor._sanitize_text("Hello\x00World") == "HelloWorld"

        # Test None input
        assert extractor._sanitize_text(None) is None



class TestAbstractRedditStorage:
    """Test cases for AbstractRedditStorage"""

    def test_abstract_storage_is_abstract(self):
        """Test that AbstractRedditStorage cannot be instantiated directly"""
        with pytest.raises(TypeError):
            AbstractRedditStorage()

    def test_abstract_storage_inheritance(self):
        """Test that AbstractRedditStorage inherits from ABC"""
        assert issubclass(AbstractRedditStorage, ABC)

    def test_concrete_storage_implementation_works(self):
        """Test that concrete implementation can be instantiated"""

        class ConcreteStorage(AbstractRedditStorage):
            def save_post(self, post):
                return True

            def save_posts(self, posts):
                return len(posts)

            def save_comment(self, comment):
                return True

            def save_comments(self, comments):
                return len(comments)

            def post_exists(self, post_id):
                return False

            def comment_exists(self, comment_id):
                return False

            def get_latest_post_timestamp(self, subreddit):
                return None

        storage = ConcreteStorage()

        # Test that all methods work
        test_post = RedditPost(id="test", title="Test Post")
        test_comment = RedditComment(id="test_comment", post_id="test")

        assert storage.save_post(test_post) is True
        assert storage.save_posts([test_post]) == 1
        assert storage.save_comment(test_comment) is True
        assert storage.save_comments([test_comment]) == 1
        assert storage.post_exists("test") is False
        assert storage.comment_exists("test_comment") is False
        assert storage.get_latest_post_timestamp("test_subreddit") is None

    def test_missing_methods_raise_error(self):
        """Test that missing abstract methods raise TypeError"""

        class IncompleteStorage(AbstractRedditStorage):
            def save_post(self, post):
                return True
            # Missing other required methods

        with pytest.raises(TypeError):
            IncompleteStorage()


class TestAbstractMethodSignatures:
    """Test that abstract method signatures are correct"""

    def test_extractor_method_signatures(self):
        """Test AbstractRedditExtractor method signatures"""

        class TestExtractor(AbstractRedditExtractor):
            def extract_posts(self, limit=10, time_filter="day"):
                # Test method signature compatibility
                assert isinstance(limit, int)
                assert isinstance(time_filter, str)
                return []

            def extract_comments(self, post_id, limit=None):
                # Test method signature compatibility
                assert isinstance(post_id, str)
                assert limit is None or isinstance(limit, int)
                return []

            def extract_posts_with_comments(self, limit=10, time_filter="day", comment_limit=None):
                # Test method signature compatibility
                assert isinstance(limit, int)
                assert isinstance(time_filter, str)
                assert comment_limit is None or isinstance(comment_limit, int)
                return {"posts": [], "comments": []}

        extractor = TestExtractor("test")

        # Test method calls work with expected parameters
        extractor.extract_posts(limit=5, time_filter="week")
        extractor.extract_comments("post123", limit=10)
        extractor.extract_posts_with_comments(limit=3, time_filter="month", comment_limit=5)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])