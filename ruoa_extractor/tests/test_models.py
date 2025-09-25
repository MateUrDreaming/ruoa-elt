import pytest
from datetime import datetime
from decimal import Decimal

from ruoa_extractor.src.core.models import RedditPost, RedditComment, Base


class TestRedditPost:
    """Test cases for RedditPost model"""

    def test_reddit_post_creation(self):
        """Test basic RedditPost creation"""
        post = RedditPost(
            id="test123",
            title="Test Post Title",
            author="test_user",
            subreddit="universityofauckland",
            score=10
        )

        assert post.id == "test123"
        assert post.title == "Test Post Title"
        assert post.author == "test_user"
        assert post.subreddit == "universityofauckland"
        assert post.score == 10

    def test_reddit_post_with_all_fields(self):
        """Test RedditPost with all optional fields"""
        created_time = datetime(2023, 1, 1, 12, 0, 0)

        post = RedditPost(
            id="full_test",
            title="Complete Test Post",
            selftext="This is the post content",
            author="full_user",
            created_utc=created_time,
            score=25,
            num_comments=5,
            upvote_ratio=Decimal("0.85"),
            url="https://reddit.com/test",
            subreddit="universityofauckland",
            flair_text="Discussion",
            flair_css_class="discussion",
            is_video=False,
            is_self=True,
            permalink="/r/test/comments/full_test/",
            post_hint="self"
        )

        assert post.selftext == "This is the post content"
        assert post.created_utc == created_time
        assert post.num_comments == 5
        assert post.upvote_ratio == Decimal("0.85")
        assert post.url == "https://reddit.com/test"
        assert post.flair_text == "Discussion"
        assert post.is_video is False
        assert post.is_self is True

    def test_reddit_post_optional_fields_none(self):
        """Test RedditPost with None values for optional fields"""
        post = RedditPost(
            id="minimal_test",
            title="Minimal Post"
        )

        assert post.id == "minimal_test"
        assert post.title == "Minimal Post"
        assert post.author is None
        assert post.selftext is None
        assert post.score is None
        assert post.created_utc is None

    def test_reddit_post_repr(self):
        """Test RedditPost string representation"""
        post = RedditPost(
            id="repr_test",
            title="This is a very long title that should be truncated in the repr method",
            author="repr_user"
        )

        repr_str = repr(post)
        assert "repr_test" in repr_str
        assert "repr_user" in repr_str
        assert "..." in repr_str  # Title should be truncated


class TestRedditComment:
    """Test cases for RedditComment model"""

    def test_reddit_comment_creation(self):
        """Test basic RedditComment creation"""
        comment = RedditComment(
            id="comment123",
            post_id="post123",
            body="This is a test comment",
            author="commenter",
            score=5
        )

        assert comment.id == "comment123"
        assert comment.post_id == "post123"
        assert comment.body == "This is a test comment"
        assert comment.author == "commenter"
        assert comment.score == 5

    def test_reddit_comment_with_all_fields(self):
        """Test RedditComment with all optional fields"""
        created_time = datetime(2023, 1, 1, 13, 0, 0)

        comment = RedditComment(
            id="full_comment",
            post_id="full_post",
            parent_id="parent123",
            body="Complete test comment",
            author="full_commenter",
            created_utc=created_time,
            score=3,
            is_submitter=True,
            permalink="/r/test/comments/full_post/full_comment/"
        )

        assert comment.parent_id == "parent123"
        assert comment.created_utc == created_time
        assert comment.is_submitter is True
        assert comment.permalink == "/r/test/comments/full_post/full_comment/"

    def test_reddit_comment_optional_fields_none(self):
        """Test RedditComment with None values"""
        comment = RedditComment(
            id="minimal_comment",
            post_id="minimal_post"
        )

        assert comment.id == "minimal_comment"
        assert comment.post_id == "minimal_post"
        assert comment.body is None
        assert comment.author is None
        assert comment.parent_id is None
        assert comment.score is None

    def test_reddit_comment_repr(self):
        """Test RedditComment string representation"""
        long_body = "This is a very long comment body that should be truncated in the repr method to keep it readable"

        comment = RedditComment(
            id="repr_comment",
            post_id="repr_post",
            body=long_body,
            author="repr_commenter"
        )

        repr_str = repr(comment)
        assert "repr_comment" in repr_str
        assert "repr_commenter" in repr_str
        assert "..." in repr_str  # Body should be truncated

    def test_reddit_comment_short_body_repr(self):
        """Test RedditComment repr with short body (no truncation)"""
        comment = RedditComment(
            id="short_comment",
            post_id="short_post",
            body="Short body",
            author="short_user"
        )

        repr_str = repr(comment)
        assert "Short body" in repr_str
        assert "..." not in repr_str  # Should not be truncated


class TestModelRelationships:
    """Test relationships between models"""

    def test_post_comment_relationship_setup(self):
        """Test that relationship attributes exist"""
        post = RedditPost(id="rel_post", title="Relationship Test")
        comment = RedditComment(id="rel_comment", post_id="rel_post")

        # Test that relationship attributes exist
        assert hasattr(post, 'comments')
        assert hasattr(comment, 'post')

        # Note: Actual relationship testing requires database session
        # which we'll test in integration tests


if __name__ == "__main__":
    pytest.main([__file__, "-v"])