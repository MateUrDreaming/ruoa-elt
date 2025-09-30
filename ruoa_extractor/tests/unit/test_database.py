import pytest
from unittest.mock import Mock, patch, MagicMock
from contextlib import contextmanager

from ruoa_extractor.src.core.database import DatabaseManager
from ruoa_extractor.src.core.models import Base


class TestDatabaseManager:

    def test_database_manager_initialization(self):
        db_url = "sqlite:///:memory:"
        db_manager = DatabaseManager(db_url)

        assert db_manager.database_url == db_url
        assert db_manager.engine is not None
        assert db_manager.SessionLocal is not None

    def test_database_manager_with_postgresql_url(self):
        db_url = "postgresql://user:pass@localhost:5432/testdb"
        db_manager = DatabaseManager(db_url)

        assert db_manager.database_url == db_url
        assert "postgresql" in str(db_manager.engine.url)

    @patch('ruoa_extractor.src.core.database.Base')
    @patch('ruoa_extractor.src.core.database.inspect')
    def test_create_tables_calls_metadata_create_all(self, mock_inspect, mock_base):
        mock_metadata = Mock()
        mock_metadata.sorted_tables = []
        mock_base.metadata = mock_metadata

        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = []
        mock_inspect.return_value = mock_inspector

        db_manager = DatabaseManager("sqlite:///:memory:")
        db_manager.create_tables()

        mock_metadata.create_all.assert_called_once_with(bind=db_manager.engine)

    def test_get_session_context_manager(self):
        db_manager = DatabaseManager("sqlite:///:memory:")
        db_manager.create_tables()  # Create tables first

        session_generator = db_manager.get_session()
        assert hasattr(session_generator, '__enter__')
        assert hasattr(session_generator, '__exit__')

        with db_manager.get_session() as session:
            assert session is not None
            assert hasattr(session, 'query')
            assert hasattr(session, 'commit')
            assert hasattr(session, 'rollback')

    def test_get_session_yields_session(self):
        db_manager = DatabaseManager("sqlite:///:memory:")

        with db_manager.get_session() as session:
            assert hasattr(session, 'add')
            assert hasattr(session, 'commit')
            assert hasattr(session, 'rollback')
            assert hasattr(session, 'query')

    @patch('ruoa_extractor.src.core.database.sessionmaker')
    def test_get_session_commits_on_success(self, mock_sessionmaker):
        mock_session = Mock()
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class

        db_manager = DatabaseManager("sqlite:///:memory:")

        with db_manager.get_session() as session:
            pass

        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    @patch('ruoa_extractor.src.core.database.sessionmaker')
    def test_get_session_rollback_on_exception(self, mock_sessionmaker):
        mock_session = Mock()
        mock_session_class = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_class

        db_manager = DatabaseManager("sqlite:///:memory:")

        with pytest.raises(ValueError):
            with db_manager.get_session() as session:
                raise ValueError("Test exception")

        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()
        mock_session.commit.assert_not_called()

    def test_database_manager_engine_properties(self):
        db_manager = DatabaseManager("sqlite:///:memory:")

        # Verify engine has required attributes/methods
        assert hasattr(db_manager.engine, 'connect')
        assert hasattr(db_manager.engine, 'url')
        assert hasattr(db_manager.engine, 'dispose')

        # Verify the URL is correct
        assert str(db_manager.engine.url) == "sqlite:///:memory:"

    def test_database_manager_different_urls(self):
        urls = [
            "sqlite:///:memory:",
            "sqlite:///test.db",
            "postgresql://user:pass@localhost/db",
        ]

        for url in urls:
            db_manager = DatabaseManager(url)
            assert db_manager.database_url == url


if __name__ == "__main__":
    pytest.main([__file__, "-v"])