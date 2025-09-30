import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from ruoa_extractor.src.pipeline.reddit_elt import RedditETLPipeline
from ruoa_extractor.src.core.models import RedditPost, RedditComment


@pytest.mark.integration
class TestPipelineIntegration:

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_full_pipeline_with_real_database(self, mock_extractor, mock_get_url, test_database):
        mock_get_url.return_value = "sqlite:///:memory:"

        mock_post = RedditPost(
            id="pipeline_test_post",
            title="Pipeline Integration Test",
            author="pipeline_author",
            subreddit="universityofauckland",
            score=15
        )

        mock_comment = RedditComment(
            id="pipeline_test_comment",
            post_id="pipeline_test_post",
            body="Pipeline test comment",
            author="comment_author",
            score=5
        )

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_posts.return_value = [mock_post]
        mock_extractor_instance.extract_comments.return_value = [mock_comment]
        mock_extractor.return_value = mock_extractor_instance

        with patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager') as mock_db_manager:
            mock_db_manager.return_value = test_database

            pipeline = RedditETLPipeline("universityofauckland", use_test_db=True)
            results = pipeline.run_full_pipeline(post_limit=1, comment_limit=1)

            assert results["posts"]["posts_saved"] == 1
            assert results["comments"]["comments_saved"] == 1
            assert results["total_data_points"] == 2
            assert "pipeline_duration_seconds" in results

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_pipeline_duplicate_handling(self, mock_extractor, mock_get_url, test_database):
        mock_get_url.return_value = "sqlite:///:memory:"

        mock_post = RedditPost(
            id="duplicate_pipeline_post",
            title="Duplicate Test Post",
            author="duplicate_author",
            subreddit="universityofauckland",
            score=20
        )

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_posts.return_value = [mock_post]
        mock_extractor_instance.extract_comments.return_value = []
        mock_extractor.return_value = mock_extractor_instance

        with patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager') as mock_db_manager:
            mock_db_manager.return_value = test_database

            pipeline = RedditETLPipeline("universityofauckland", use_test_db=True)

            first_run = pipeline.extract_and_load_posts(limit=1)
            assert first_run["posts_saved"] == 1
            assert first_run["posts_skipped"] == 0

            second_run = pipeline.extract_and_load_posts(limit=1)
            assert second_run["posts_saved"] == 0
            assert second_run["posts_skipped"] == 1

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_pipeline_statistics_tracking(self, mock_extractor, mock_get_url, test_database):
        mock_get_url.return_value = "sqlite:///:memory:"

        posts = [
            RedditPost(
                id=f"stats_post_{i}",
                title=f"Stats Post {i}",
                subreddit="universityofauckland",
                score=i * 10,
                created_utc=datetime(2023, 1, i)
            )
            for i in range(1, 4)
        ]

        comments_post_1 = [
            RedditComment(id=f"stats_comment_1_{i}", post_id="stats_post_1", body=f"Comment {i}", score=i * 2)
            for i in range(1, 3)
        ]
        comments_post_2 = [
            RedditComment(id=f"stats_comment_2_{i}", post_id="stats_post_2", body=f"Comment {i}", score=i * 2)
            for i in range(1, 3)
        ]
        comments_post_3 = [
            RedditComment(id=f"stats_comment_3_{i}", post_id="stats_post_3", body=f"Comment {i}", score=i * 2)
            for i in range(1, 3)
        ]

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_posts.return_value = posts
        mock_extractor_instance.extract_comments.side_effect = [comments_post_1, comments_post_2, comments_post_3]
        mock_extractor.return_value = mock_extractor_instance

        with patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager') as mock_db_manager:
            mock_db_manager.return_value = test_database

            pipeline = RedditETLPipeline("universityofauckland", use_test_db=True)

            initial_stats = pipeline.get_pipeline_stats()
            assert initial_stats["total_posts"] == 0
            assert initial_stats["total_comments"] == 0

            pipeline.run_full_pipeline(post_limit=3, comment_limit=2)

            final_stats = pipeline.get_pipeline_stats()
            assert final_stats["total_posts"] == 3
            assert final_stats["total_comments"] == 6
            assert final_stats["subreddit"] == "universityofauckland"

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_pipeline_error_recovery(self, mock_extractor, mock_get_url, test_database):
        mock_get_url.return_value = "sqlite:///:memory:"

        valid_post = RedditPost(
            id="valid_error_post",
            title="Valid Post",
            subreddit="universityofauckland",
            score=10
        )

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_posts.return_value = [valid_post]
        mock_extractor_instance.extract_comments.side_effect = Exception("Extraction error")
        mock_extractor.return_value = mock_extractor_instance

        with patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager') as mock_db_manager:
            mock_db_manager.return_value = test_database

            pipeline = RedditETLPipeline("universityofauckland", use_test_db=True)

            post_results = pipeline.extract_and_load_posts(limit=1)
            assert post_results["posts_saved"] == 1


            comment_results = pipeline.extract_and_load_comments(post_ids=["valid_error_post"])
            assert comment_results["comments_saved"] == 0
            assert comment_results["posts_processed"] == 1

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_pipeline_incremental_extraction(self, mock_extractor, mock_get_url, test_database):
        mock_get_url.return_value = "sqlite:///:memory:"

        older_post = RedditPost(
            id="older_incremental_post",
            title="Older Post",
            subreddit="universityofauckland",
            created_utc=datetime(2023, 1, 1)
        )

        newer_post = RedditPost(
            id="newer_incremental_post",
            title="Newer Post",
            subreddit="universityofauckland",
            created_utc=datetime(2023, 1, 2)
        )

        mock_extractor_instance = Mock()
        mock_extractor.return_value = mock_extractor_instance

        with patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager') as mock_db_manager:
            mock_db_manager.return_value = test_database

            pipeline = RedditETLPipeline("universityofauckland", use_test_db=True)

            mock_extractor_instance.extract_posts.return_value = [older_post]
            mock_extractor_instance.extract_comments.return_value = []

            first_results = pipeline.run_full_pipeline(post_limit=1)
            assert first_results["posts"]["posts_saved"] == 1

            latest_timestamp = pipeline.storage.get_latest_post_timestamp("universityofauckland")
            assert latest_timestamp is not None

            mock_extractor_instance.extract_posts.return_value = [newer_post]

            second_results = pipeline.run_full_pipeline(post_limit=1)
            assert second_results["posts"]["posts_saved"] == 1

            final_stats = pipeline.get_pipeline_stats()
            assert final_stats["total_posts"] == 2

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_pipeline_with_empty_extraction(self, mock_extractor, mock_get_url, test_database):
        mock_get_url.return_value = "sqlite:///:memory:"

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_posts.return_value = []
        mock_extractor_instance.extract_comments.return_value = []
        mock_extractor.return_value = mock_extractor_instance

        with patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager') as mock_db_manager:
            mock_db_manager.return_value = test_database

            pipeline = RedditETLPipeline("universityofauckland", use_test_db=True)
            results = pipeline.run_full_pipeline()

            assert results["posts"]["posts_saved"] == 0
            assert results["posts"]["total_extracted"] == 0
            assert results["comments"]["comments_saved"] == 0
            assert results["total_data_points"] == 0

    @patch('ruoa_extractor.src.pipeline.reddit_elt.get_database_url')
    @patch('ruoa_extractor.src.pipeline.reddit_elt.PrawRedditExtractor')
    def test_pipeline_comment_auto_discovery(self, mock_extractor, mock_get_url, test_database):
        mock_get_url.return_value = "sqlite:///:memory:"

        existing_post = RedditPost(
            id="existing_discovery_post",
            title="Existing Post for Discovery",
            subreddit="universityofauckland",
            created_utc=datetime(2023, 1, 1)
        )

        comment_for_existing = RedditComment(
            id="discovery_comment",
            post_id="existing_discovery_post",
            body="Comment for existing post",
            score=3
        )

        mock_extractor_instance = Mock()
        mock_extractor_instance.extract_comments.return_value = [comment_for_existing]
        mock_extractor.return_value = mock_extractor_instance

        with patch('ruoa_extractor.src.pipeline.reddit_elt.DatabaseManager') as mock_db_manager:
            mock_db_manager.return_value = test_database

            pipeline = RedditETLPipeline("universityofauckland", use_test_db=True)

            pipeline.storage.save_post(existing_post)

            comment_results = pipeline.extract_and_load_comments()

            assert comment_results["comments_saved"] == 1
            assert comment_results["posts_processed"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])