"""
Reddit ETL Pipeline - Main Application Entry Point

This application extracts posts and comments from r/universityofauckland
and loads them into a PostgreSQL database for analysis.
"""

import argparse
import sys
import logging
from typing import Dict, Any

from ruoa_extractor.src.pipeline.reddit_elt import RedditETLPipeline
from ruoa_extractor.src.config.config import get_reddit_settings, get_database_url


def setup_logging(log_level: str = "INFO") -> None:
    """Setup application logging"""
    level = getattr(logging, log_level.upper())

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('reddit_etl.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def check_configuration() -> bool:
    """Check if application is properly configured"""
    logger = logging.getLogger(__name__)

    reddit_settings = get_reddit_settings()
    if not reddit_settings.is_configured():
        logger.error("Reddit API credentials not configured!")
        logger.error("Please set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in your .env file")
        return False

    logger.info("Reddit API credentials configured")

    try:
        db_url = get_database_url(use_test_db=False)
        logger.info(f"Database configured: {db_url.split('@')[0]}@[HIDDEN]")
    except Exception as e:
        logger.error(f"Database configuration error: {e}")
        return False

    return True


def run_single_extraction(
        subreddit: str = "universityofauckland",
        post_limit: int = 25,
        time_filter: str = "day",
        comment_limit: int = None,
        use_test_db: bool = False
) -> Dict[str, Any]:
    """Run a single extraction cycle"""
    logger = logging.getLogger(__name__)

    logger.info(f"Starting single extraction for r/{subreddit}")
    logger.info(f"Parameters: posts={post_limit}, filter={time_filter}, comments={comment_limit}")

    try:
        pipeline = RedditETLPipeline(subreddit, use_test_db=use_test_db)

        stats = pipeline.get_pipeline_stats()
        logger.info(f"Current stats - Posts: {stats['total_posts']}, Comments: {stats['total_comments']}")

        results = pipeline.run_full_pipeline(
            post_limit=post_limit,
            time_filter=time_filter,
            comment_limit=comment_limit
        )

        duration = results['pipeline_duration_seconds']
        total_data = results['total_data_points']

        logger.info(f"Extraction completed in {duration:.2f}s")
        logger.info(f"Data extracted: {total_data} total items")
        logger.info(f"Posts: {results['posts']['posts_saved']} saved, {results['posts']['posts_skipped']} skipped")
        logger.info(
            f"Comments: {results['comments']['comments_saved']} saved, {results['comments']['comments_skipped']} skipped")

        return results

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


def run_continuous_mode(
        subreddit: str = "universityofauckland",
        interval_hours: int = 12,
        post_limit: int = 25,
        time_filter: str = "day",
        comment_limit: int = None
) -> None:
    """Run continuous extraction every N hours"""
    import time

    logger = logging.getLogger(__name__)
    logger.info(f"Starting continuous mode - every {interval_hours} hours")

    interval_seconds = interval_hours * 3600

    try:
        while True:
            try:
                results = run_single_extraction(
                    subreddit=subreddit,
                    post_limit=post_limit,
                    time_filter=time_filter,
                    comment_limit=comment_limit,
                    use_test_db=False
                )

                logger.info(f"Sleeping for {interval_hours} hours until next extraction...")
                time.sleep(interval_seconds)

            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping continuous mode")
                break
            except Exception as e:
                logger.error(f"Error in continuous cycle: {e}")
                logger.info(f"Waiting {interval_hours} hours before retry...")
                time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Continuous mode stopped by user")


def show_stats(subreddit: str = "universityofauckland", use_test_db: bool = False) -> None:
    """Show current pipeline statistics"""
    logger = logging.getLogger(__name__)

    try:
        pipeline = RedditETLPipeline(subreddit, use_test_db=use_test_db)
        stats = pipeline.get_pipeline_stats()

        print(f"\nPipeline Statistics for r/{subreddit}")
        print("=" * 50)
        print(f"Total Posts: {stats['total_posts']:,}")
        print(f"Total Comments: {stats['total_comments']:,}")
        print(f"Latest Post: {stats['latest_post_timestamp']}")
        print(f"Database: {stats['database_url'].split('@')[0]}@[HIDDEN]")
        print("=" * 50)

    except Exception as e:
        logger.error(f"Error getting stats: {e}")


def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(
        description="Reddit ETL Pipeline for r/universityofauckland",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py extract                           # Extract 25 posts from today
  python main.py extract --posts 50 --filter week # Extract 50 posts from this week
  python main.py extract --comments 20            # Limit comments per post to 20
  python main.py continuous --interval 6          # Run every 6 hours
  python main.py stats                             # Show current statistics
  python main.py extract --test                   # Use test database
        """
    )

    parser.add_argument(
        'command',
        choices=['extract', 'continuous', 'stats'],
        help='Command to run'
    )

    parser.add_argument(
        '--subreddit',
        default='universityofauckland',
        help='Subreddit to extract from (default: universityofauckland)'
    )

    parser.add_argument(
        '--posts',
        type=int,
        default=25,
        help='Number of posts to extract (default: 25)'
    )

    parser.add_argument(
        '--filter',
        choices=['hour', 'day', 'week', 'month', 'year', 'all'],
        default='day',
        help='Time filter for posts (default: day)'
    )

    parser.add_argument(
        '--comments',
        type=int,
        help='Limit comments per post (default: no limit)'
    )

    parser.add_argument(
        '--interval',
        type=int,
        default=12,
        help='Hours between extractions in continuous mode (default: 12)'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='Use test database instead of production'
    )

    args = parser.parse_args()

    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    logger.info(f"Starting Reddit ETL Pipeline - Command: {args.command}")

    if not check_configuration():
        logger.error("Configuration check failed. Please fix configuration and try again.")
        sys.exit(1)

    try:
        if args.command == 'extract':
            run_single_extraction(
                subreddit=args.subreddit,
                post_limit=args.posts,
                time_filter=args.filter,
                comment_limit=args.comments,
                use_test_db=args.test
            )

        elif args.command == 'continuous':
            if args.test:
                logger.warning("Continuous mode should not use test database. Using production database.")

            run_continuous_mode(
                subreddit=args.subreddit,
                interval_hours=args.interval,
                post_limit=args.posts,
                time_filter=args.filter,
                comment_limit=args.comments
            )

        elif args.command == 'stats':
            show_stats(args.subreddit, use_test_db=args.test)

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)

    logger.info("Application finished")


if __name__ == "__main__":
    main()
