import pytest
from datetime import datetime
from decimal import Decimal

from ruoa_extractor.src.core.database import DatabaseManager
from ruoa_extractor.src.core.models import RedditPost, RedditComment


@pytest.mark.integration
class TestDatabaseIntegration:

    def test_database_table_creation(self, test_database):
        from sqlalchemy import inspect

        inspector = inspect(test_database.engine)
        tables = inspector.get_table_names()

        assert "raw_reddit_posts" in tables
        assert "raw_reddit_comments" in tables

    def test_post_crud_operations(self, test_database):
        post = RedditPost(
            id="integration_post_123",
            title="Integration Test Post",
            selftext="Test content for integration",
            author="integration_user",
            created_utc=datetime(2023, 1, 1, 12, 0, 0),
            score=25,
            num_comments=3,
            upvote_ratio=Decimal("0.87"),
            url="https://reddit.com/integration_test",
            subreddit="universityofauckland",
            flair_text="Test",
            is_self=True,
            permalink="/r/universityofauckland/integration_post_123/"
        )

        with test_database.get_session() as session:
            session.add(post)
            session.flush()

            retrieved_post = session.query(RedditPost).filter_by(id="integration_post_123").first()

            assert retrieved_post is not None
            assert retrieved_post.id == "integration_post_123"
            assert retrieved_post.title == "Integration Test Post"
            assert retrieved_post.author == "integration_user"
            assert retrieved_post.score == 25
            assert retrieved_post.upvote_ratio == Decimal("0.87")
            assert retrieved_post.is_self is True

    def test_comment_crud_operations(self, test_database):
        post = RedditPost(
            id="parent_post_123",
            title="Parent Post",
            subreddit="universityofauckland"
        )

        comment = RedditComment(
            id="integration_comment_456",
            post_id="parent_post_123",
            parent_id="parent_post_123",
            body="Integration test comment",
            author="comment_author",
            created_utc=datetime(2023, 1, 1, 13, 0, 0),
            score=5,
            is_submitter=False,
            permalink="/r/universityofauckland/parent_post_123/integration_comment_456/"
        )

        with test_database.get_session() as session:
            session.add(post)
            session.add(comment)
            session.flush()

            retrieved_comment = session.query(RedditComment).filter_by(id="integration_comment_456").first()

            assert retrieved_comment is not None
            assert retrieved_comment.id == "integration_comment_456"
            assert retrieved_comment.post_id == "parent_post_123"
            assert retrieved_comment.body == "Integration test comment"
            assert retrieved_comment.author == "comment_author"
            assert retrieved_comment.is_submitter is False

    def test_post_comment_relationship(self, test_database):
        post = RedditPost(
            id="relationship_post_123",
            title="Relationship Test Post",
            subreddit="universityofauckland"
        )

        comment1 = RedditComment(
            id="rel_comment_1",
            post_id="relationship_post_123",
            body="First comment"
        )

        comment2 = RedditComment(
            id="rel_comment_2",
            post_id="relationship_post_123",
            body="Second comment"
        )

        with test_database.get_session() as session:
            session.add(post)
            session.add(comment1)
            session.add(comment2)
            session.flush()

            retrieved_post = session.query(RedditPost).filter_by(id="relationship_post_123").first()

            assert len(retrieved_post.comments) == 2
            comment_ids = [c.id for c in retrieved_post.comments]
            assert "rel_comment_1" in comment_ids
            assert "rel_comment_2" in comment_ids

            retrieved_comment = session.query(RedditComment).filter_by(id="rel_comment_1").first()
            assert retrieved_comment.post.id == "relationship_post_123"

    def test_database_session_rollback_on_error(self, test_database):
        with pytest.raises(Exception):
            with test_database.get_session() as session:
                post = RedditPost(
                    id="error_post_123",
                    title="Error Test Post",
                    subreddit="universityofauckland"
                )
                session.add(post)
                session.flush()

                raise Exception("Simulated database error")

        with test_database.get_session() as session:
            retrieved_post = session.query(RedditPost).filter_by(id="error_post_123").first()
            assert retrieved_post is None

    def test_duplicate_post_handling(self, test_database):
        post1 = RedditPost(
            id="duplicate_test_123",
            title="Original Post",
            subreddit="universityofauckland"
        )

        post2 = RedditPost(
            id="duplicate_test_123",
            title="Updated Post",
            subreddit="universityofauckland",
            score=10
        )

        with test_database.get_session() as session:
            session.add(post1)
            session.flush()

        with test_database.get_session() as session:
            session.merge(post2)
            session.flush()

            retrieved_post = session.query(RedditPost).filter_by(id="duplicate_test_123").first()
            assert retrieved_post.title == "Updated Post"
            assert retrieved_post.score == 10

    def test_query_filtering_and_ordering(self, test_database):
        posts = [
            RedditPost(
                id=f"filter_post_{i}",
                title=f"Post {i}",
                subreddit="universityofauckland",
                score=i * 10,
                created_utc=datetime(2023, 1, i + 1)
            )
            for i in range(1, 4)
        ]

        with test_database.get_session() as session:
            for post in posts:
                session.add(post)
            session.flush()

            high_score_posts = (session.query(RedditPost)
                                .filter(RedditPost.score > 15)
                                .order_by(RedditPost.score.desc())
                                .all())

            assert len(high_score_posts) == 2
            assert high_score_posts[0].score == 30
            assert high_score_posts[1].score == 20

            subreddit_posts = (session.query(RedditPost)
                               .filter_by(subreddit="universityofauckland")
                               .count())

            assert subreddit_posts == 3

    def test_null_field_handling(self, test_database):
        post = RedditPost(
            id="null_test_123",
            title="Null Field Test"
        )

        comment = RedditComment(
            id="null_comment_123",
            post_id="null_test_123"
        )

        with test_database.get_session() as session:
            session.add(post)
            session.add(comment)
            session.flush()

            retrieved_post = session.query(RedditPost).filter_by(id="null_test_123").first()
            retrieved_comment = session.query(RedditComment).filter_by(id="null_comment_123").first()

            assert retrieved_post.author is None
            assert retrieved_post.score is None
            assert retrieved_post.created_utc is None

            assert retrieved_comment.body is None
            assert retrieved_comment.author is None
            assert retrieved_comment.parent_id is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])