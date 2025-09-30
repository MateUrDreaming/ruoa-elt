import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from ruoa_extractor.src.extractors.praw_extractor import PrawRedditExtractor


class TestPrawRedditExtractor:

    @patch('ruoa_extractor.src.extractors.praw_extractor.get_reddit_settings')
    @patch('ruoa_extractor.src.extractors.praw_extractor.praw.Reddit')
    def test_extractor_initialization(self, mock_reddit, mock_get_settings):
        mock_settings = Mock()
        mock_settings.is_configured.return_value = True
        mock_settings.client_id = "test_id"
        mock_settings.client_secret = "test_secret"
        mock_settings.user_agent = "test_agent"
        mock_get_settings.return_value = mock_settings

        mock_reddit_instance = Mock()
        mock_reddit.return_value = mock_reddit_instance
        mock_reddit_instance.subreddit.return_value = Mock()

        extractor = PrawRedditExtractor("test_subreddit")

        assert extractor.subreddit_name == "test_subreddit"
        assert extractor.reddit_settings == mock_settings
        mock_reddit.assert_called_once_with(
            client_id="test_id",
            client_secret="test_secret",
            user_agent="test_agent"
        )

    @patch('ruoa_extractor.src.extractors.praw_extractor.get_reddit_settings')
    def test_extractor_initialization_not_configured(self, mock_get_settings):
        mock_settings = Mock()
        mock_settings.is_configured.return_value = False
        mock_get_settings.return_value = mock_settings

        with pytest.raises(ValueError, match="Reddit API credentials not properly configured"):
            PrawRedditExtractor("test_subreddit")

    @patch('ruoa_extractor.src.extractors.praw_extractor.get_reddit_settings')
    @patch('ruoa_extractor.src.extractors.praw_extractor.praw.Reddit')
    def test_extract_posts(self, mock_reddit, mock_get_settings):
        mock_settings = Mock()
        mock_settings.is_configured.return_value = True
        mock_get_settings.return_value = mock_settings

        mock_submission = Mock()
        mock_submission.id = "post123"
        mock_submission.title = "Test Post"
        mock_submission.selftext = "Post content"
        mock_submission.author = Mock()
        mock_submission.author.__str__ = Mock(return_value="test_author")
        mock_submission.created_utc = 1640995200.0
        mock_submission.score = 10
        mock_submission.num_comments = 5
        mock_submission.upvote_ratio = 0.8
        mock_submission.url = "https://reddit.com/test"
        mock_submission.subreddit = Mock()
        mock_submission.subreddit.__str__ = Mock(return_value="test_subreddit")
        mock_submission.link_flair_text = "Test Flair"
        mock_submission.link_flair_css_class = "test-class"
        mock_submission.is_video = False
        mock_submission.is_self = True
        mock_submission.permalink = "/r/test/post123"
        mock_submission.post_hint = "self"

        mock_subreddit = Mock()
        mock_subreddit.top.return_value = [mock_submission]

        mock_reddit_instance = Mock()
        mock_reddit_instance.subreddit.return_value = mock_subreddit
        mock_reddit.return_value = mock_reddit_instance

        extractor = PrawRedditExtractor("test_subreddit")
        posts = extractor.extract_posts(limit=1, time_filter="day")

        assert len(posts) == 1
        post = posts[0]
        assert post.id == "post123"
        assert post.title == "Test Post"
        assert post.author == "test_author"
        assert post.score == 10
        assert post.subreddit == "test_subreddit"

        mock_subreddit.top.assert_called_once_with(time_filter="day", limit=1)

    @patch('ruoa_extractor.src.extractors.praw_extractor.isinstance')
    @patch('ruoa_extractor.src.extractors.praw_extractor.get_reddit_settings')
    @patch('ruoa_extractor.src.extractors.praw_extractor.praw.Reddit')
    def test_extract_comments(self, mock_reddit, mock_get_settings, mock_isinstance):
        mock_isinstance.return_value = True

        mock_settings = Mock()
        mock_settings.is_configured.return_value = True
        mock_get_settings.return_value = mock_settings

        mock_comment = Mock()
        mock_comment.id = "comment123"
        mock_comment.parent_id = "post123"
        mock_comment.body = "Test comment"
        mock_comment.author = Mock()
        mock_comment.author.__str__ = Mock(return_value="commenter")
        mock_comment.created_utc = 1640998800.0
        mock_comment.score = 3
        mock_comment.is_submitter = False
        mock_comment.permalink = "/r/test/comment123"

        mock_submission = Mock()
        mock_submission.comments = Mock()
        mock_submission.comments.replace_more = Mock()
        mock_submission.comments.list.return_value = [mock_comment]

        mock_reddit_instance = Mock()
        mock_reddit_instance.submission.return_value = mock_submission
        mock_reddit.return_value = mock_reddit_instance
        mock_reddit_instance.subreddit.return_value = Mock()

        extractor = PrawRedditExtractor("test_subreddit")
        comments = extractor.extract_comments("post123", limit=10)

        assert len(comments) == 1
        comment = comments[0]
        assert comment.id == "comment123"
        assert comment.post_id == "post123"
        assert comment.body == "Test comment"
        assert comment.author == "commenter"
        assert comment.score == 3

        mock_reddit_instance.submission.assert_called_once_with(id="post123")
        mock_submission.comments.replace_more.assert_called_once_with(limit=0)

    @patch('ruoa_extractor.src.extractors.praw_extractor.get_reddit_settings')
    @patch('ruoa_extractor.src.extractors.praw_extractor.praw.Reddit')
    def test_extract_posts_with_comments(self, mock_reddit, mock_get_settings):
        mock_settings = Mock()
        mock_settings.is_configured.return_value = True
        mock_get_settings.return_value = mock_settings

        mock_reddit_instance = Mock()
        mock_reddit.return_value = mock_reddit_instance
        mock_reddit_instance.subreddit.return_value = Mock()

        extractor = PrawRedditExtractor("test_subreddit")

        with patch.object(extractor, 'extract_posts') as mock_extract_posts, \
                patch.object(extractor, 'extract_comments') as mock_extract_comments:
            mock_post = Mock()
            mock_post.id = "post123"
            mock_extract_posts.return_value = [mock_post]

            mock_comment = Mock()
            mock_extract_comments.return_value = [mock_comment]

            result = extractor.extract_posts_with_comments(
                limit=1, time_filter="day", comment_limit=5
            )

            assert result["total_posts"] == 1
            assert result["total_comments"] == 1
            assert "posts" in result
            assert "comments" in result

            mock_extract_posts.assert_called_once_with(1, "day")
            mock_extract_comments.assert_called_once_with("post123", 5)

    def test_convert_timestamp(self):
        extractor = PrawRedditExtractor.__new__(PrawRedditExtractor)
        extractor.subreddit_name = "test"

        unix_timestamp = 1640995200.0
        result = extractor._convert_timestamp(unix_timestamp)

        assert isinstance(result, datetime)
        assert result.year == 2022
        assert result.month == 1
        assert result.day == 1

    def test_sanitize_text(self):
        extractor = PrawRedditExtractor.__new__(PrawRedditExtractor)
        extractor.subreddit_name = "test"

        assert extractor._sanitize_text("Hello World") == "Hello World"
        assert extractor._sanitize_text("  Hello World  ") == "Hello World"
        assert extractor._sanitize_text("Hello\x00World") == "HelloWorld"
        assert extractor._sanitize_text(None) is None

    @patch('ruoa_extractor.src.extractors.praw_extractor.get_reddit_settings')
    @patch('ruoa_extractor.src.extractors.praw_extractor.praw.Reddit')
    def test_submission_to_model_with_none_author(self, mock_reddit, mock_get_settings):
        mock_settings = Mock()
        mock_settings.is_configured.return_value = True
        mock_get_settings.return_value = mock_settings

        mock_submission = Mock()
        mock_submission.id = "post123"
        mock_submission.title = "Test Post"
        mock_submission.selftext = "Content"
        mock_submission.author = None
        mock_submission.created_utc = 1640995200.0
        mock_submission.score = 10
        mock_submission.num_comments = 0
        mock_submission.upvote_ratio = 1.0
        mock_submission.url = "https://reddit.com"
        mock_submission.subreddit = Mock()
        mock_submission.subreddit.__str__ = Mock(return_value="test")
        mock_submission.link_flair_text = None
        mock_submission.link_flair_css_class = None
        mock_submission.is_video = False
        mock_submission.is_self = True
        mock_submission.permalink = "/test"
        mock_submission.post_hint = None

        mock_reddit_instance = Mock()
        mock_reddit.return_value = mock_reddit_instance
        mock_reddit_instance.subreddit.return_value = Mock()

        extractor = PrawRedditExtractor("test")
        post = extractor._submission_to_model(mock_submission)

        assert post.author is None
        assert post.id == "post123"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])