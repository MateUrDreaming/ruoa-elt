from ruoa_extractor.src.config.config import get_database_url
from ruoa_extractor.src.core.database import DatabaseManager
from sqlalchemy import text


def debug_database_connection():
    print("=== Database Connection Debug ===")

    # Check database URL
    db_url = get_database_url(use_test_db=False)
    print(f"Database URL: {db_url}")

    # Test connection
    try:
        db_manager = DatabaseManager(db_url)

        print("✅ DatabaseManager created successfully")
        print(f"Database Connection on {db_manager.database_url}")
        # Test engine connection with SQLAlchemy 2.0 syntax
        with db_manager.engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ PostgreSQL connection successful: {version}")

        # Check current tables
        from sqlalchemy import inspect
        inspector = inspect(db_manager.engine)
        existing_tables = inspector.get_table_names()
        print(f"📋 Existing tables: {existing_tables}")

        # Try to create tables
        print("🔨 Attempting to create tables...")
        db_manager.create_tables()
        print("✅ create_tables() completed without errors")

        # Check tables again
        inspector = inspect(db_manager.engine)
        existing_tables = inspector.get_table_names()
        print(f"📋 Tables after creation: {existing_tables}")

        if 'raw_reddit_posts' in existing_tables:
            print("✅ raw_reddit_posts table created successfully")
        else:
            print("❌ raw_reddit_posts table NOT created")

        if 'raw_reddit_comments' in existing_tables:
            print("✅ raw_reddit_comments table created successfully")
        else:
            print("❌ raw_reddit_comments table NOT created")

    except Exception as e:
        print(f"❌ Database error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    db_manager = DatabaseManager(get_database_url(use_test_db=False))
    db_manager.create_tables()

