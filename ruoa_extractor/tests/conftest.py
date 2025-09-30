import pytest
import tempfile
import os
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, patch

from ruoa_extractor.src.core.models import RedditPost, RedditComment, Base
from ruoa_extractor.src.core.database import DatabaseManager
from ruoa_extractor.src.config.config import get_database_url


@pytest.fixture
def sample_reddit_post():
    return RedditPost(
        id="sample_post_123",
        title="Sample Post Title for Testing",
        selftext="This is sample post content for testing purposes",
        author="sample_user",
        created_utc=datetime(2023, 1, 1, 12, 0, 0),
        score=15,
        num_comments=3,
        upvote_ratio=Decimal("0.85"),
        url="https://reddit.com/r/universityofauckland/sample",
        subreddit="universityofauckland",
        flair_text="Discussion",
        flair_css_class="discussion",
        is_video=False,
        is_self=True,
        permalink="/r/universityofauckland/comments/sample_post_123/",
        post_hint="self"
    )


@pytest.fixture
def sample_reddit_comment(sample_reddit_post):
    return RedditComment(
        id="sample_comment_456",
        post_id=sample_reddit_post.id,
        parent_id=sample_reddit_post.id,
        body="This is a sample comment for testing purposes",
        author="comment_user",
        created_utc=datetime(2023, 1, 1, 12, 30, 0),
        score=5,
        is_submitter=False,
        permalink="/r/universityofauckland/comments/sample_post_123/sample_comment_456/"
    )


@pytest.fixture
def mock_reddit_settings():
    with patch('ruoa_extractor.src.config.settings.get_reddit_settings') as mock:
        mock_settings = Mock()
        mock_settings.client_id = "test_client_id"
        mock_settings.client_secret = "test_client_secret"
        mock_settings.user_agent = "test-agent/1.0"
        mock_settings.is_configured.return_value = True
        mock.return_value = mock_settings
        yield mock_settings


@pytest.fixture
def mock_database_settings():
    with patch('ruoa_extractor.src.config.settings.get_database_url') as mock:
        mock.return_value = "sqlite:///:memory:"
        yield mock


@pytest.fixture
def test_database():
    db_manager = DatabaseManager("sqlite:///:memory:")
    db_manager.create_tables()

    yield db_manager


@pytest.fixture
def test_database_with_data(test_database, sample_reddit_post, sample_reddit_comment):
    with test_database.get_session() as session:
        session.add(sample_reddit_post)
        session.add(sample_reddit_comment)
        session.commit()

    yield test_database


@pytest.fixture
def mock_praw_submission():
    mock_submission = Mock()
    mock_submission.id = "mock_post_123"
    mock_submission.title = "Mock Post Title"
    mock_submission.selftext = "Mock post content"
    mock_submission.author = Mock()
    mock_submission.author.__str__ = Mock(return_value="mock_author")
    mock_submission.created_utc = 1640995200.0
    mock_submission.score = 10
    mock_submission.num_comments = 2
    mock_submission.upvote_ratio = 0.8
    mock_submission.url = "https://reddit.com/mock"
    mock_submission.subreddit = Mock()
    mock_submission.subreddit.__str__ = Mock(return_value="universityofauckland")
    mock_submission.link_flair_text = "Test Flair"
    mock_submission.link_flair_css_class = "test-flair"
    mock_submission.is_video = False
    mock_submission.is_self = True
    mock_submission.permalink = "/r/universityofauckland/comments/mock_post_123/"
    mock_submission.post_hint = "self"

    return mock_submission


@pytest.fixture
def mock_praw_comment():
    mock_comment = Mock()
    mock_comment.id = "mock_comment_456"
    mock_comment.parent_id = "mock_post_123"
    mock_comment.body = "Mock comment body"
    mock_comment.author = Mock()
    mock_comment.author.__str__ = Mock(return_value="mock_commenter")
    mock_comment.created_utc = 1640998800.0
    mock_comment.score = 3
    mock_comment.is_submitter = False
    mock_comment.permalink = "/r/universityofauckland/comments/mock_post_123/mock_comment_456/"

    return mock_comment


@pytest.fixture
def mock_env_vars():
    env_vars = {
        "REDDIT_CLIENT_ID": "test_client_id",
        "REDDIT_CLIENT_SECRET": "test_client_secret",
        "REDDIT_USER_AGENT": "test-agent/1.0",
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "POSTGRES_DB": "test_db",
        "DB_HOST": "localhost",
        "DB_PORT": "5432"
    }

    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def temp_log_file():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        yield f.name

    if os.path.exists(f.name):
        os.unlink(f.name)


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )