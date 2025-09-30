import pytest
from unittest.mock import Mock, patch, call, MagicMock
from datetime import datetime

from ruoa_extractor.src.pipeline.reddit_elt import RedditETLPipeline


class TestRedditETLPipeline:

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseRedditStorage')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_pipeline_initialization(self, mock_extractor, mock_storage, mock_db_manager, mock_get_url):
        mock_get_url.return_value = "sqlite:///:memory:"
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance

        pipeline = RedditETLPipeline("test_subreddit", use_test_db=True)

        assert pipeline.subreddit_name == "test_subreddit"
        assert pipeline.use_test_db is True
        mock_extractor.assert_called_once_with("test_subreddit")
        assert mock_db_manager.call_count == 2
        mock_db_manager.assert_called_with("sqlite:///:memory:")
        mock_db_instance.create_tables.assert_called_once()

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseRedditStorage')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_extract_and_load_posts_success(self, mock_extractor, mock_storage, mock_db_manager, mock_get_url):
        mock_get_url.return_value = "sqlite:///:memory:"
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance

        mock_post1 = Mock()
        mock_post1.id = "post1"
        mock_post2 = Mock()
        mock_post2.id = "post2"

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_posts.return_value = [mock_post1, mock_post2]
        mock_extractor.return_value = mock_extractor_instance

        mock_storage_instance = Mock()
        mock_storage_instance.post_exists.side_effect = [False, True]
        mock_storage_instance.save_post.return_value = True
        mock_storage.return_value = mock_storage_instance

        pipeline = RedditETLPipeline("test_subreddit")
        result = pipeline.extract_and_load_posts(limit=2, time_filter="day")

        assert result["posts_saved"] == 1
        assert result["posts_skipped"] == 1
        assert result["total_extracted"] == 2
        mock_extractor_instance.extract_posts.assert_called_once_with(limit=2, time_filter="day")
        mock_storage_instance.save_post.assert_called_once_with(mock_post1)

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseRedditStorage')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_extract_and_load_posts_no_posts(self, mock_extractor, mock_storage, mock_db_manager, mock_get_url):
        mock_get_url.return_value = "sqlite:///:memory:"
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_posts.return_value = []
        mock_extractor.return_value = mock_extractor_instance

        mock_storage.return_value = Mock()

        pipeline = RedditETLPipeline("test_subreddit")
        result = pipeline.extract_and_load_posts()

        assert result["posts_saved"] == 0
        assert result["posts_skipped"] == 0
        assert result["total_extracted"] == 0

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseRedditStorage')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_extract_and_load_comments_with_post_ids(self, mock_extractor, mock_storage, mock_db_manager, mock_get_url):
        mock_get_url.return_value = "sqlite:///:memory:"
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance

        mock_comment1 = Mock()
        mock_comment1.id = "comment1"
        mock_comment2 = Mock()
        mock_comment2.id = "comment2"

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_comments.return_value = [mock_comment1, mock_comment2]
        mock_extractor.return_value = mock_extractor_instance

        mock_storage_instance = Mock()
        mock_storage_instance.comment_exists.side_effect = [False, False]
        mock_storage_instance.save_comment.return_value = True
        mock_storage.return_value = mock_storage_instance

        pipeline = RedditETLPipeline("test_subreddit")
        result = pipeline.extract_and_load_comments(post_ids=["post1"], comment_limit=10)

        assert result["comments_saved"] == 2
        assert result["comments_skipped"] == 0
        assert result["total_extracted"] == 2
        assert result["posts_processed"] == 1
        mock_extractor_instance.extract_comments.assert_called_once_with("post1", limit=10)

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseRedditStorage')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_extract_and_load_comments_auto_post_discovery(self, mock_extractor, mock_storage, mock_db_manager,
                                                           mock_get_url):
        mock_get_url.return_value = "sqlite:///:memory:"
        mock_db_instance = MagicMock()  # Changed from Mock() to MagicMock()
        mock_session = Mock()
        mock_db_instance.get_session.return_value.__enter__.return_value = mock_session
        mock_db_manager.return_value = mock_db_instance

        mock_post = Mock()
        mock_post.id = "discovered_post"
        mock_session.query.return_value.filter_by.return_value.order_by.return_value.limit.return_value.all.return_value = [
            mock_post]

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_comments.return_value = []
        mock_extractor.return_value = mock_extractor_instance

        mock_storage.return_value = Mock()

        pipeline = RedditETLPipeline("test_subreddit")
        result = pipeline.extract_and_load_comments()

        assert result["posts_processed"] == 1
        mock_extractor_instance.extract_comments.assert_called_once_with("discovered_post", limit=None)

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseRedditStorage')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_run_full_pipeline_success(self, mock_extractor, mock_storage, mock_db_manager, mock_get_url):
        mock_get_url.return_value = "sqlite:///:memory:"
        mock_db_instance = MagicMock()  # Changed from Mock()
        mock_session = Mock()
        mock_db_instance.get_session.return_value.__enter__.return_value = mock_session
        mock_db_manager.return_value = mock_db_instance

        mock_post = Mock()
        mock_post.id = "new_post"
        mock_session.query.return_value.filter_by.return_value.order_by.return_value.limit.return_value.all.return_value = [
            mock_post]

        mock_storage.return_value = Mock()

        pipeline = RedditETLPipeline("test_subreddit")

        with patch.object(pipeline, 'extract_and_load_posts') as mock_extract_posts, \
                patch.object(pipeline, 'extract_and_load_comments') as mock_extract_comments:
            mock_extract_posts.return_value = {"posts_saved": 2, "posts_skipped": 1, "total_extracted": 3}
            mock_extract_comments.return_value = {"comments_saved": 5, "comments_skipped": 2, "total_extracted": 7,
                                                  "posts_processed": 2}

            result = pipeline.run_full_pipeline(post_limit=10, time_filter="day", comment_limit=20)

            assert result["posts"]["posts_saved"] == 2
            assert result["comments"]["comments_saved"] == 5
            assert result["total_data_points"] == 7
            assert "pipeline_duration_seconds" in result

            mock_extract_posts.assert_called_once_with(limit=10, time_filter="day")
            mock_extract_comments.assert_called_once_with(post_ids=["new_post"], comment_limit=20)

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseRedditStorage')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_run_full_pipeline_no_new_posts(self, mock_extractor, mock_storage, mock_db_manager, mock_get_url):
        mock_get_url.return_value = "sqlite:///:memory:"
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance

        mock_storage.return_value = Mock()

        pipeline = RedditETLPipeline("test_subreddit")

        with patch.object(pipeline, 'extract_and_load_posts') as mock_extract_posts:
            mock_extract_posts.return_value = {"posts_saved": 0, "posts_skipped": 3, "total_extracted": 3}

            result = pipeline.run_full_pipeline()

            assert result["posts"]["posts_saved"] == 0
            assert result["comments"]["comments_saved"] == 0
            assert result["total_data_points"] == 0

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseRedditStorage')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_get_pipeline_stats(self, mock_extractor, mock_storage, mock_db_manager, mock_get_url):
        mock_get_url.return_value = "sqlite:///:memory:"
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance

        mock_storage_instance = Mock()
        mock_storage_instance.get_post_count.return_value = 100
        mock_storage_instance.get_comment_count.return_value = 500
        mock_storage_instance.get_latest_post_timestamp.return_value = 1640995200.0
        mock_storage.return_value = mock_storage_instance

        pipeline = RedditETLPipeline("test_subreddit", use_test_db=True)
        stats = pipeline.get_pipeline_stats()

        assert stats["subreddit"] == "test_subreddit"
        assert stats["total_posts"] == 100
        assert stats["total_comments"] == 500
        assert stats["latest_post_timestamp"] == 1640995200.0
        assert stats["database_url"] == "sqlite:///:memory:"

        mock_storage_instance.get_post_count.assert_called_once_with("test_subreddit")
        mock_storage_instance.get_comment_count.assert_called_once_with("test_subreddit")
        mock_storage_instance.get_latest_post_timestamp.assert_called_once_with("test_subreddit")

    @patch('ruoa_extractor.src.pipeline.reddit_elt.logging')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseRedditStorage')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_pipeline_logging_setup(self, mock_extractor, mock_storage, mock_db_manager, mock_get_url, mock_logging):
        mock_get_url.return_value = "sqlite:///:memory:"
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance

        mock_logger = Mock()
        mock_logging.getLogger.return_value = mock_logger

        pipeline = RedditETLPipeline("test_subreddit")

        mock_logging.basicConfig.assert_called_once()
        mock_logging.getLogger.assert_called_with("RedditETL-test_subreddit")
        assert pipeline.logger == mock_logger


if __name__ == "__main__":
    pytest.main([__file__, "-v"])