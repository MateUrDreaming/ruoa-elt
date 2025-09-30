from typing import List, Optional, Dict, Any
import praw
from praw.models import Submission, Comment

from ruoa_extractor.src.extractors.abstract_extractor import AbstractRedditExtractor
from ruoa_extractor.src.core.models import RedditPost, RedditComment
from ruoa_extractor.src.config.config import get_reddit_settings


class PrawRedditExtractor(AbstractRedditExtractor):
    """Reddit extractor using PRAW (Python Reddit API Wrapper)"""

    def __init__(self, subreddit_name: str):
        super().__init__(subreddit_name)
        self.reddit_settings = get_reddit_settings()
        self.reddit = self._initialize_reddit_client()
        self.subreddit = self.reddit.subreddit(subreddit_name)

    def _initialize_reddit_client(self) -> praw.Reddit:
        """Initialize PRAW Reddit client with credentials (read-only access)"""
        if not self.reddit_settings.is_configured():
            raise ValueError("Reddit API credentials not properly configured")

        return praw.Reddit(
            client_id=self.reddit_settings.client_id,
            client_secret=self.reddit_settings.client_secret,
            user_agent=self.reddit_settings.user_agent,
        )

    def extract_posts(self, limit: int = 10, time_filter: str = "day") -> List[RedditPost]:
        """Extract Reddit posts from the subreddit"""
        posts = []

        submissions = self.subreddit.top(time_filter=time_filter, limit=limit)

        for submission in submissions:
            post = self._submission_to_model(submission)
            posts.append(post)

        return posts

    def extract_comments(self, post_id: str, limit: Optional[int] = None) -> List[RedditComment]:
        """Extract comments for a specific post"""
        comments = []

        submission = self.reddit.submission(id=post_id)
        submission.comments.replace_more(limit=0)

        comment_list = submission.comments.list()
        if limit:
            comment_list = comment_list[:limit]

        for comment in comment_list:
            if isinstance(comment, Comment):
                comment_model = self._comment_to_model(comment, post_id)
                comments.append(comment_model)

        return comments

    def extract_posts_with_comments(
            self,
            limit: int = 10,
            time_filter: str = "day",
            comment_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Extract posts along with their comments"""
        posts = self.extract_posts(limit, time_filter)
        all_comments = []

        for post in posts:
            post_comments = self.extract_comments(post.id, comment_limit)
            all_comments.extend(post_comments)

        return {
            "posts": posts,
            "comments": all_comments,
            "total_posts": len(posts),
            "total_comments": len(all_comments)
        }

    def _submission_to_model(self, submission: Submission) -> RedditPost:
        """Convert PRAW submission to RedditPost model"""
        return RedditPost(
            id=submission.id,
            title=self._sanitize_text(submission.title),
            selftext=self._sanitize_text(submission.selftext),
            author=str(submission.author) if submission.author else None,
            created_utc=self._convert_timestamp(submission.created_utc),
            score=submission.score,
            num_comments=submission.num_comments,
            upvote_ratio=submission.upvote_ratio,
            url=submission.url,
            subreddit=str(submission.subreddit),
            flair_text=submission.link_flair_text,
            flair_css_class=submission.link_flair_css_class,
            is_video=submission.is_video,
            is_self=submission.is_self,
            permalink=submission.permalink,
            post_hint=getattr(submission, 'post_hint', None),
        )

    def _comment_to_model(self, comment: Comment, post_id: str) -> RedditComment:
        """Convert PRAW comment to RedditComment model"""
        return RedditComment(
            id=comment.id,
            post_id=post_id,
            parent_id=comment.parent_id,
            body=self._sanitize_text(comment.body),
            author=str(comment.author) if comment.author else None,
            created_utc=self._convert_timestamp(comment.created_utc),
            score=comment.score,
            is_submitter=comment.is_submitter,
            permalink=comment.permalink,
        )


if __name__ == "__main__":
    print("Testing PrawRedditExtractor...")

    try:
        extractor = PrawRedditExtractor("universityofauckland")
        print(f"✅ PRAW extractor initialized for r/{extractor.subreddit_name}")
        print("Note: Add Reddit API credentials to .env to test extraction")
    except Exception as e:
        print(f"❌ Error initializing PRAW: {e}")
        print("Make sure to set REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, etc. in .env")