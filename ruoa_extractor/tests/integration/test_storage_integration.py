import pytest
from datetime import datetime
from decimal import Decimal

from ruoa_extractor.src.storage.database_storage import DatabaseRedditStorage
from ruoa_extractor.src.core.models import RedditPost, RedditComment


@pytest.mark.integration
class TestDatabaseStorageIntegration:

    def test_storage_initialization_with_real_database(self, test_database):
        storage = DatabaseRedditStorage(test_database)
        assert storage.db_manager == test_database

    def test_save_and_retrieve_single_post(self, test_database):
        storage = DatabaseRedditStorage(test_database)

        post = RedditPost(
            id="storage_integration_post_123",
            title="Storage Integration Test Post",
            selftext="Integration test content",
            author="integration_author",
            created_utc=datetime(2023, 6, 1, 14, 30, 0),
            score=42,
            num_comments=7,
            upvote_ratio=Decimal("0.92"),
            subreddit="universityofauckland",
            is_self=True
        )

        result = storage.save_post(post)
        assert result is True

        exists = storage.post_exists("storage_integration_post_123")
        assert exists is True

        with test_database.get_session() as session:
            retrieved = session.query(RedditPost).filter_by(id="storage_integration_post_123").first()
            assert retrieved is not None
            assert retrieved.title == "Storage Integration Test Post"
            assert retrieved.score == 42

    def test_save_multiple_posts_batch(self, test_database):
        storage = DatabaseRedditStorage(test_database)

        posts = [
            RedditPost(
                id=f"batch_post_{i}",
                title=f"Batch Post {i}",
                author=f"author_{i}",
                score=i * 5,
                subreddit="universityofauckland"
            )
            for i in range(1, 6)
        ]

        saved_count = storage.save_posts(posts)
        assert saved_count == 5

        for i in range(1, 6):
            assert storage.post_exists(f"batch_post_{i}") is True

    def test_save_and_retrieve_single_comment(self, test_database):
        storage = DatabaseRedditStorage(test_database)

        post = RedditPost(
            id="comment_parent_post",
            title="Parent for Comment Test",
            subreddit="universityofauckland"
        )
        storage.save_post(post)

        comment = RedditComment(
            id="storage_integration_comment_456",
            post_id="comment_parent_post",
            body="Integration test comment body",
            author="comment_integration_author",
            created_utc=datetime(2023, 6, 1, 15, 0, 0),
            score=8,
            is_submitter=False
        )

        result = storage.save_comment(comment)
        assert result is True

        exists = storage.comment_exists("storage_integration_comment_456")
        assert exists is True

        with test_database.get_session() as session:
            retrieved = session.query(RedditComment).filter_by(id="storage_integration_comment_456").first()
            assert retrieved is not None
            assert retrieved.body == "Integration test comment body"
            assert retrieved.post_id == "comment_parent_post"

    def test_save_multiple_comments_batch(self, test_database):
        storage = DatabaseRedditStorage(test_database)

        post = RedditPost(
            id="batch_comment_parent",
            title="Parent for Batch Comments",
            subreddit="universityofauckland"
        )
        storage.save_post(post)

        comments = [
            RedditComment(
                id=f"batch_comment_{i}",
                post_id="batch_comment_parent",
                body=f"Batch comment {i}",
                author=f"commenter_{i}",
                score=i * 2
            )
            for i in range(1, 4)
        ]

        saved_count = storage.save_comments(comments)
        assert saved_count == 3

        for i in range(1, 4):
            assert storage.comment_exists(f"batch_comment_{i}") is True

    def test_duplicate_post_handling_with_upsert(self, test_database):
        storage = DatabaseRedditStorage(test_database)

        original_post = RedditPost(
            id="upsert_test_post",
            title="Original Title",
            score=10,
            subreddit="universityofauckland"
        )

        storage.save_post(original_post)
        assert storage.post_exists("upsert_test_post") is True

        updated_post = RedditPost(
            id="upsert_test_post",
            title="Updated Title",
            score=20,
            subreddit="universityofauckland"
        )

        result = storage.save_post(updated_post)
        assert result is True

        with test_database.get_session() as session:
            retrieved = session.query(RedditPost).filter_by(id="upsert_test_post").first()
            assert retrieved.title == "Updated Title"
            assert retrieved.score == 20

    def test_get_latest_post_timestamp(self, test_database):
        storage = DatabaseRedditStorage(test_database)

        posts = [
            RedditPost(
                id="timestamp_post_1",
                title="First Post",
                subreddit="universityofauckland",
                created_utc=datetime(2023, 1, 1, 10, 0, 0)
            ),
            RedditPost(
                id="timestamp_post_2",
                title="Latest Post",
                subreddit="universityofauckland",
                created_utc=datetime(2023, 1, 2, 15, 30, 0)
            ),
            RedditPost(
                id="timestamp_post_3",
                title="Different Subreddit",
                subreddit="different_subreddit",
                created_utc=datetime(2023, 1, 3, 20, 0, 0)
            )
        ]

        for post in posts:
            storage.save_post(post)

        latest_timestamp = storage.get_latest_post_timestamp("universityofauckland")
        expected_timestamp = datetime(2023, 1, 2, 15, 30, 0).timestamp()

        assert latest_timestamp == expected_timestamp

        no_posts_timestamp = storage.get_latest_post_timestamp("nonexistent_subreddit")
        assert no_posts_timestamp is None

    def test_get_post_and_comment_counts(self, test_database):
        storage = DatabaseRedditStorage(test_database)

        posts = [
            RedditPost(
                id=f"count_post_{i}",
                title=f"Count Post {i}",
                subreddit="universityofauckland"
            )
            for i in range(1, 4)
        ]

        for post in posts:
            storage.save_post(post)

        comments = [
            RedditComment(
                id=f"count_comment_{i}",
                post_id="count_post_1",
                body=f"Count comment {i}"
            )
            for i in range(1, 6)
        ]

        for comment in comments:
            storage.save_comment(comment)

        post_count = storage.get_post_count("universityofauckland")
        comment_count = storage.get_comment_count("universityofauckland")

        assert post_count == 3
        assert comment_count == 5

        zero_post_count = storage.get_post_count("nonexistent_subreddit")
        zero_comment_count = storage.get_comment_count("nonexistent_subreddit")

        assert zero_post_count == 0
        assert zero_comment_count == 0

    def test_error_handling_during_save(self, test_database):
        storage = DatabaseRedditStorage(test_database)

        invalid_post = RedditPost(
            id="invalid_post",
            title=None
        )

        result = storage.save_post(invalid_post)
        assert result is False

        exists = storage.post_exists("invalid_post")
        assert exists is False

    def test_foreign_key_constraint_with_comments(self, test_database):
        storage = DatabaseRedditStorage(test_database)

        post = RedditPost(
            id="fk_test_post",
            title="FK Test Post",
            subreddit="universityofauckland"
        )
        storage.save_post(post)

        valid_comment = RedditComment(
            id="valid_fk_comment",
            post_id="fk_test_post",
            body="Valid comment"
        )

        result = storage.save_comment(valid_comment)
        assert result is True
        assert storage.comment_exists("valid_fk_comment") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])