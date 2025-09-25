import pytest
import os
from unittest.mock import patch

from ruoa_extractor.src.config.config import (
    DatabaseSettings,
    RedditSettings,
    get_database_url,
    get_reddit_settings
)


class TestDatabaseSettings:
    """Test cases for DatabaseSettings"""

    def test_database_settings_defaults(self):
        """Test DatabaseSettings with default values"""
        with patch.dict(os.environ, {}, clear=True):
            db_settings = DatabaseSettings()

            assert db_settings.host == "localhost"
            assert db_settings.port == 5432
            assert db_settings.name == "ruoa"
            assert db_settings.user == "postgres"
            assert db_settings.password == "postgres"

    def test_database_settings_from_env(self):
        """Test DatabaseSettings reads from environment variables"""
        env_vars = {
            "DB_HOST": "custom_host",
            "DB_PORT": "9999",
            "POSTGRES_DB": "custom_db",
            "POSTGRES_USER": "custom_user",
            "POSTGRES_PASSWORD": "custom_pass"
        }

        with patch.dict(os.environ, env_vars):
            db_settings = DatabaseSettings()

            assert db_settings.host == "custom_host"
            assert db_settings.port == 9999
            assert db_settings.name == "custom_db"
            assert db_settings.user == "custom_user"
            assert db_settings.password == "custom_pass"

    def test_database_url_property(self):
        """Test DatabaseSettings URL generation"""
        with patch.dict(os.environ, {
            "DB_HOST": "testhost",
            "DB_PORT": "5433",
            "POSTGRES_DB": "testdb",
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass"
        }):
            db_settings = DatabaseSettings()
            expected_url = "postgresql+psycopg2://testuser:testpass@testhost:5433/testdb"
            assert db_settings.url == expected_url

    def test_database_test_url_property(self):
        """Test DatabaseSettings test URL"""
        db_settings = DatabaseSettings()
        assert db_settings.test_url == "sqlite:///test_reddit.db"

    def test_database_port_conversion(self):
        """Test that DB_PORT is converted to integer"""
        with patch.dict(os.environ, {"DB_PORT": "8080"}):
            db_settings = DatabaseSettings()
            assert db_settings.port == 8080
            assert isinstance(db_settings.port, int)


class TestRedditSettings:
    """Test cases for RedditSettings"""

    def test_reddit_settings_defaults(self):
        """Test RedditSettings with default values"""
        with patch.dict(os.environ, {}, clear=True):
            reddit_settings = RedditSettings()

            assert reddit_settings.client_id is None
            assert reddit_settings.client_secret is None
            assert reddit_settings.user_agent == "pk-uoa-etl/1.0"

    def test_reddit_settings_from_env(self):
        """Test RedditSettings reads from environment variables"""
        env_vars = {
            "REDDIT_CLIENT_ID": "test_client_id",
            "REDDIT_CLIENT_SECRET": "test_client_secret",
            "REDDIT_USER_AGENT": "test-agent/2.0"
        }

        with patch.dict(os.environ, env_vars):
            reddit_settings = RedditSettings()

            assert reddit_settings.client_id == "test_client_id"
            assert reddit_settings.client_secret == "test_client_secret"
            assert reddit_settings.user_agent == "test-agent/2.0"

    def test_reddit_settings_is_configured_true(self):
        """Test is_configured returns True when all required fields are set"""
        env_vars = {
            "REDDIT_CLIENT_ID": "test_id",
            "REDDIT_CLIENT_SECRET": "test_secret"
        }

        with patch.dict(os.environ, env_vars):
            reddit_settings = RedditSettings()
            assert reddit_settings.is_configured() is True

    def test_reddit_settings_is_configured_false(self):
        """Test is_configured returns False when required fields are missing"""
        # Test with no environment variables
        with patch.dict(os.environ, {}, clear=True):
            reddit_settings = RedditSettings()
            assert reddit_settings.is_configured() is False

        # Test with only client_id
        with patch.dict(os.environ, {"REDDIT_CLIENT_ID": "test_id"}, clear=True):
            reddit_settings = RedditSettings()
            assert reddit_settings.is_configured() is False

        # Test with only client_secret
        with patch.dict(os.environ, {"REDDIT_CLIENT_SECRET": "test_secret"}, clear=True):
            reddit_settings = RedditSettings()
            assert reddit_settings.is_configured() is False


class TestConfigurationFunctions:
    """Test configuration utility functions"""

    def test_get_database_url_production(self):
        """Test get_database_url with production database"""
        env_vars = {
            "DB_HOST": "prod_host",
            "DB_PORT": "5432",
            "POSTGRES_DB": "prod_db",
            "POSTGRES_USER": "prod_user",
            "POSTGRES_PASSWORD": "prod_pass"
        }

        with patch.dict(os.environ, env_vars):
            url = get_database_url(use_test_db=False)
            expected = "postgresql+psycopg2://prod_user:prod_pass@prod_host:5432/prod_db"
            assert url == expected

    def test_get_database_url_test(self):
        """Test get_database_url with test database"""
        url = get_database_url(use_test_db=True)
        assert url == "sqlite:///test_reddit.db"

    def test_get_reddit_settings_function(self):
        """Test get_reddit_settings function"""
        env_vars = {
            "REDDIT_CLIENT_ID": "func_test_id",
            "REDDIT_CLIENT_SECRET": "func_test_secret"
        }

        with patch.dict(os.environ, env_vars):
            reddit_settings = get_reddit_settings()

            assert isinstance(reddit_settings, RedditSettings)
            assert reddit_settings.client_id == "func_test_id"
            assert reddit_settings.client_secret == "func_test_secret"
            assert reddit_settings.is_configured() is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])