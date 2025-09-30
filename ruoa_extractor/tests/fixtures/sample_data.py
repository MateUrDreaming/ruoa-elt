from datetime import datetime
from decimal import Decimal
from typing import List

from ruoa_extractor.src.core.models import RedditPost, RedditComment


def create_sample_posts() -> List[RedditPost]:
    return [
        RedditPost(
            id="sample_post_1",
            title="First Sample Post - Course Registration Help",
            selftext="I need help with course registration for COMPSCI 101. When do they open?",
            author="student_user_1",
            created_utc=datetime(2023, 6, 15, 9, 0, 0),
            score=25,
            num_comments=8,
            upvote_ratio=Decimal("0.88"),
            url="https://reddit.com/r/universityofauckland/sample_post_1",
            subreddit="universityofauckland",
            flair_text="Question",
            flair_css_class="question",
            is_video=False,
            is_self=True,
            permalink="/r/universityofauckland/comments/sample_post_1/course_registration/",
            post_hint="self"
        ),
        RedditPost(
            id="sample_post_2",
            title="University Library Hours During Exam Period",
            selftext="Does anyone know if General Library extends hours during exams?",
            author="study_buddy",
            created_utc=datetime(2023, 6, 16, 14, 30, 0),
            score=42,
            num_comments=12,
            upvote_ratio=Decimal("0.95"),
            url="https://reddit.com/r/universityofauckland/sample_post_2",
            subreddit="universityofauckland",
            flair_text="Question",
            flair_css_class="question",
            is_video=False,
            is_self=True,
            permalink="/r/universityofauckland/comments/sample_post_2/library_hours/",
            post_hint="self"
        ),
        RedditPost(
            id="sample_post_3",
            title="Campus Food Recommendations",
            selftext="Best places to eat on campus? Looking for affordable options.",
            author="hungry_student",
            created_utc=datetime(2023, 6, 17, 11, 15, 0),
            score=18,
            num_comments=15,
            upvote_ratio=Decimal("0.82"),
            url="https://reddit.com/r/universityofauckland/sample_post_3",
            subreddit="universityofauckland",
            flair_text="Discussion",
            flair_css_class="discussion",
            is_video=False,
            is_self=True,
            permalink="/r/universityofauckland/comments/sample_post_3/food_recommendations/",
            post_hint="self"
        )
    ]


def create_sample_comments(post_id: str = "sample_post_1") -> List[RedditComment]:
    return [
        RedditComment(
            id="sample_comment_1",
            post_id=post_id,
            parent_id=post_id,
            body="Registration opens on July 1st at 9am. Make sure you log in early!",
            author="helpful_senior",
            created_utc=datetime(2023, 6, 15, 9, 30, 0),
            score=12,
            is_submitter=False,
            permalink=f"/r/universityofauckland/comments/{post_id}/sample_comment_1/"
        ),
        RedditComment(
            id="sample_comment_2",
            post_id=post_id,
            parent_id="sample_comment_1",
            body="Thanks! Do you know if there are any prerequisites?",
            author="student_user_1",
            created_utc=datetime(2023, 6, 15, 10, 0, 0),
            score=3,
            is_submitter=True,
            permalink=f"/r/universityofauckland/comments/{post_id}/sample_comment_2/"
        ),
        RedditComment(
            id="sample_comment_3",
            post_id=post_id,
            parent_id=post_id,
            body="COMPSCI 101 has no prerequisites. It's designed for beginners.",
            author="cs_tutor",
            created_utc=datetime(2023, 6, 15, 11, 0, 0),
            score=8,
            is_submitter=False,
            permalink=f"/r/universityofauckland/comments/{post_id}/sample_comment_3/"
        ),
        RedditComment(
            id="sample_comment_4",
            post_id=post_id,
            parent_id="sample_comment_3",
            body="Perfect! I'll register as soon as it opens.",
            author="student_user_1",
            created_utc=datetime(2023, 6, 15, 11, 30, 0),
            score=2,
            is_submitter=True,
            permalink=f"/r/universityofauckland/comments/{post_id}/sample_comment_4/"
        )
    ]


def create_large_dataset() -> tuple[List[RedditPost], List[RedditComment]]:
    posts = []
    comments = []

    for i in range(1, 51):
        post = RedditPost(
            id=f"bulk_post_{i}",
            title=f"Bulk Test Post {i}",
            selftext=f"This is bulk test content for post {i}",
            author=f"bulk_author_{i % 10}",
            created_utc=datetime(2023, 6, 1 + (i % 30), 12, 0, 0),
            score=i * 2,
            num_comments=i % 5,
            upvote_ratio=Decimal(str(0.7 + (i % 3) * 0.1)),
            url=f"https://reddit.com/bulk_post_{i}",
            subreddit="universityofauckland",
            is_self=True,
            permalink=f"/r/universityofauckland/comments/bulk_post_{i}/"
        )
        posts.append(post)

        for j in range(1, (i % 5) + 1):
            comment = RedditComment(
                id=f"bulk_comment_{i}_{j}",
                post_id=f"bulk_post_{i}",
                parent_id=f"bulk_post_{i}",
                body=f"Bulk comment {j} for post {i}",
                author=f"bulk_commenter_{j % 10}",
                created_utc=datetime(2023, 6, 1 + (i % 30), 13, j, 0),
                score=j,
                is_submitter=False,
                permalink=f"/r/universityofauckland/bulk_post_{i}/bulk_comment_{i}_{j}/"
            )
            comments.append(comment)

    return posts, comments


def create_posts_for_subreddit(subreddit: str, count: int = 10) -> List[RedditPost]:
    return [
        RedditPost(
            id=f"{subreddit}_post_{i}",
            title=f"Post {i} from r/{subreddit}",
            selftext=f"Content for post {i}",
            author=f"author_{i}",
            created_utc=datetime(2023, 6, 1 + i, 10, 0, 0),
            score=i * 5,
            num_comments=i,
            upvote_ratio=Decimal("0.85"),
            subreddit=subreddit,
            is_self=True,
            permalink=f"/r/{subreddit}/comments/{subreddit}_post_{i}/"
        )
        for i in range(1, count + 1)
    ]


def create_posts_with_different_scores() -> List[RedditPost]:
    scores = [1, 5, 10, 25, 50, 100, 500, 1000]
    return [
        RedditPost(
            id=f"score_post_{score}",
            title=f"Post with {score} score",
            author="score_tester",
            score=score,
            subreddit="universityofauckland",
            created_utc=datetime(2023, 6, 1 + i, 10, 0, 0),
            is_self=True
        )
        for i, score in enumerate(scores, 1)
    ]


def create_posts_with_different_timestamps() -> List[RedditPost]:
    return [
        RedditPost(
            id="timestamp_post_jan",
            title="January Post",
            subreddit="universityofauckland",
            created_utc=datetime(2023, 1, 15, 10, 0, 0),
            is_self=True
        ),
        RedditPost(
            id="timestamp_post_mar",
            title="March Post",
            subreddit="universityofauckland",
            created_utc=datetime(2023, 3, 15, 10, 0, 0),
            is_self=True
        ),
        RedditPost(
            id="timestamp_post_jun",
            title="June Post",
            subreddit="universityofauckland",
            created_utc=datetime(2023, 6, 15, 10, 0, 0),
            is_self=True
        ),
        RedditPost(
            id="timestamp_post_dec",
            title="December Post",
            subreddit="universityofauckland",
            created_utc=datetime(2023, 12, 15, 10, 0, 0),
            is_self=True
        )
    ]


def create_nested_comment_thread(post_id: str = "nested_post") -> List[RedditComment]:
    return [
        RedditComment(
            id="nested_comment_1",
            post_id=post_id,
            parent_id=post_id,
            body="Top level comment",
            author="commenter_1",
            created_utc=datetime(2023, 6, 15, 10, 0, 0),
            score=10,
            is_submitter=False,
            permalink=f"/r/universityofauckland/{post_id}/nested_comment_1/"
        ),
        RedditComment(
            id="nested_comment_2",
            post_id=post_id,
            parent_id="nested_comment_1",
            body="First level reply",
            author="commenter_2",
            created_utc=datetime(2023, 6, 15, 10, 15, 0),
            score=5,
            is_submitter=False,
            permalink=f"/r/universityofauckland/{post_id}/nested_comment_2/"
        ),
        RedditComment(
            id="nested_comment_3",
            post_id=post_id,
            parent_id="nested_comment_2",
            body="Second level reply",
            author="commenter_3",
            created_utc=datetime(2023, 6, 15, 10, 30, 0),
            score=2,
            is_submitter=False,
            permalink=f"/r/universityofauckland/{post_id}/nested_comment_3/"
        ),
        RedditComment(
            id="nested_comment_4",
            post_id=post_id,
            parent_id=post_id,
            body="Another top level comment",
            author="commenter_4",
            created_utc=datetime(2023, 6, 15, 11, 0, 0),
            score=8,
            is_submitter=False,
            permalink=f"/r/universityofauckland/{post_id}/nested_comment_4/"
        )
    ]