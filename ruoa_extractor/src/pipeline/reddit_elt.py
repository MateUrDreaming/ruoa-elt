from typing import Dict, Any, Optional
import logging
from datetime import datetime

from ruoa_extractor.src.extractors.praw_extractor import PrawRedditExtractor
from ruoa_extractor.src.storage.database_storage import DatabaseRedditStorage
from ruoa_extractor.src.core.database import DatabaseManager
from ruoa_extractor.src.config.config import get_database_url


class RedditETLPipeline:
    """Complete ETL pipeline for Reddit data extraction, transformation, and loading"""

    def __init__(self, subreddit_name: str, use_test_db: bool = False):
        self.subreddit_name = subreddit_name
        self.use_test_db = use_test_db

        self.extractor = PrawRedditExtractor(subreddit_name)

        db_url = get_database_url(use_test_db=use_test_db)
        print("db_url", db_url)
        self.database_manager = DatabaseManager(db_url)
        self.db_manager = DatabaseManager(db_url)
        self.storage = DatabaseRedditStorage(self.db_manager)

        self._setup_logging()

        self.db_manager.create_tables()

    def _setup_logging(self) -> None:
        """Setup logging for the pipeline"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f"RedditETL-{self.subreddit_name}")

    def extract_and_load_posts(
            self,
            limit: int = 25,
            time_filter: str = "day"
    ) -> Dict[str, Any]:
        """Extract posts and load them into database"""
        self.logger.info(f"Starting post extraction - limit: {limit}, filter: {time_filter}")

        try:
            posts = self.extractor.extract_posts(limit=limit, time_filter=time_filter)
            self.logger.info(f"Extracted {len(posts)} posts from r/{self.subreddit_name}")

            if not posts:
                self.logger.warning("No posts extracted")
                return {"posts_saved": 0, "posts_skipped": 0, "total_extracted": 0}

            posts_saved = 0
            posts_skipped = 0

            for post in posts:
                if self.storage.post_exists(post.id):
                    posts_skipped += 1
                    self.logger.debug(f"Post {post.id} already exists, skipping")
                else:
                    if self.storage.save_post(post):
                        posts_saved += 1
                        self.logger.debug(f"Saved post: {post.id}")
                    else:
                        self.logger.error(f"Failed to save post: {post.id}")

            result = {
                "posts_saved": posts_saved,
                "posts_skipped": posts_skipped,
                "total_extracted": len(posts)
            }

            self.logger.info(f"Post extraction completed: {result}")
            return result

        except Exception as e:
            self.logger.error(f"Error in post extraction: {e}")
            raise

    def extract_and_load_comments(
            self,
            post_ids: Optional[list] = None,
            comment_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Extract comments for posts and load them into database"""

        if post_ids is None:
            with self.db_manager.get_session() as session:
                from ruoa_extractor.src.core.models import RedditPost
                recent_posts = (session.query(RedditPost)
                                .filter_by(subreddit=self.subreddit_name)
                                .order_by(RedditPost.created_utc.desc())
                                .limit(10)
                                .all())
                post_ids = [post.id for post in recent_posts]

        self.logger.info(f"Starting comment extraction for {len(post_ids)} posts")

        try:
            total_comments = 0
            comments_saved = 0
            comments_skipped = 0

            for post_id in post_ids:
                try:
                    comments = self.extractor.extract_comments(post_id, limit=comment_limit)
                    total_comments += len(comments)

                    for comment in comments:
                        if self.storage.comment_exists(comment.id):
                            comments_skipped += 1
                            self.logger.debug(f"Comment {comment.id} already exists")
                        else:
                            if self.storage.save_comment(comment):
                                comments_saved += 1
                                self.logger.debug(f"Saved comment: {comment.id}")
                            else:
                                self.logger.error(f"Failed to save comment: {comment.id}")

                except Exception as e:
                    self.logger.error(f"Error extracting comments for post {post_id}: {e}")
                    continue

            result = {
                "comments_saved": comments_saved,
                "comments_skipped": comments_skipped,
                "total_extracted": total_comments,
                "posts_processed": len(post_ids)
            }

            self.logger.info(f"Comment extraction completed: {result}")
            return result

        except Exception as e:
            self.logger.error(f"Error in comment extraction: {e}")
            raise

    def run_full_pipeline(
            self,
            post_limit: int = 25,
            time_filter: str = "day",
            comment_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Run the complete ETL pipeline - extract posts and their comments"""
        self.logger.info(f"Starting full ETL pipeline for r/{self.subreddit_name}")

        pipeline_start = datetime.now()

        try:
            post_results = self.extract_and_load_posts(
                limit=post_limit,
                time_filter=time_filter
            )

            if post_results["posts_saved"] > 0:
                with self.db_manager.get_session() as session:
                    from ruoa_extractor.src.core.models import RedditPost
                    recent_posts = (session.query(RedditPost.id)
                                    .filter_by(subreddit=self.subreddit_name)
                                    .order_by(RedditPost.extraction_timestamp.desc())
                                    .limit(post_results["posts_saved"])
                                    .all())
                    new_post_ids = [post.id for post in recent_posts]

                comment_results = self.extract_and_load_comments(
                    post_ids=new_post_ids,
                    comment_limit=comment_limit
                )
            else:
                self.logger.info("No new posts saved, skipping comment extraction")
                comment_results = {
                    "comments_saved": 0,
                    "comments_skipped": 0,
                    "total_extracted": 0,
                    "posts_processed": 0
                }

            pipeline_end = datetime.now()
            duration = (pipeline_end - pipeline_start).total_seconds()

            final_results = {
                "pipeline_duration_seconds": duration,
                "posts": post_results,
                "comments": comment_results,
                "total_data_points": (
                        post_results["posts_saved"] +
                        comment_results["comments_saved"]
                )
            }

            self.logger.info(f"Full pipeline completed in {duration:.2f}s: {final_results}")
            return final_results

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise

    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get current statistics about the data in the pipeline"""
        post_count = self.storage.get_post_count(self.subreddit_name)
        comment_count = self.storage.get_comment_count(self.subreddit_name)
        latest_timestamp = self.storage.get_latest_post_timestamp(self.subreddit_name)

        return {
            "subreddit": self.subreddit_name,
            "total_posts": post_count,
            "total_comments": comment_count,
            "latest_post_timestamp": latest_timestamp,
            "database_url": get_database_url(self.use_test_db)
        }


if __name__ == "__main__":
    print("🚀 Testing Reddit ETL Pipeline...")

    pipeline = RedditETLPipeline("universityofauckland", use_test_db=False)

    print("📊 Current pipeline stats:")
    stats = pipeline.get_pipeline_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")

    print("\n🔄 Running full pipeline...")
    try:
        results = pipeline.run_full_pipeline(
            post_limit=5,
            time_filter="week",
            comment_limit=10
        )

        print("✅ Pipeline completed successfully!")
        print(f"📈 Results: {results}")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        import traceback

        traceback.print_exc()