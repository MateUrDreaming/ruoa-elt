
CREATE TABLE raw_reddit_posts (
    id VARCHAR PRIMARY KEY,
    title TEXT NOT NULL,
    selftext TEXT,
    author VARCHAR,
    created_utc TIMESTAMP,
    score INTEGER,
    num_comments INTEGER,
    upvote_ratio DECIMAL(4,3),
    url TEXT,
    subreddit VARCHAR,
    flair_text VARCHAR,
    flair_css_class VARCHAR,
    is_video BOOLEAN,
    is_self BOOLEAN,
    permalink VARCHAR,
    post_hint VARCHAR,
    extraction_timestamp TIMESTAMP DEFAULT NOW()
);


CREATE TABLE raw_reddit_comments (
    id VARCHAR PRIMARY KEY,
    post_id VARCHAR REFERENCES raw_reddit_posts(id),
    parent_id VARCHAR,
    body TEXT,
    author VARCHAR,
    created_utc TIMESTAMP,
    score INTEGER,
    is_submitter BOOLEAN,
    permalink VARCHAR,
    extraction_timestamp TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_posts_created_utc ON raw_reddit_posts(created_utc);
CREATE INDEX idx_posts_score ON raw_reddit_posts(score);
CREATE INDEX idx_posts_subreddit ON raw_reddit_posts(subreddit);
CREATE INDEX idx_comments_post_id ON raw_reddit_comments(post_id);
CREATE INDEX idx_comments_created_utc ON raw_reddit_comments(created_utc);