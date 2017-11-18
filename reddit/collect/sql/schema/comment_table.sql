-- Create a table to hold subreddit comments
-- Also make an entry in our reddit_subreddits metatable
CREATE TABLE subreddit (
    id character varying(15) NOT NULL,
    subreddit character varying(50),
    parent_id character varying(15),
    link_id character varying(15),
    author character varying(20),
    created timestamp without time zone,
    created_utc timestamp without time zone,
    author_flair_text character varying(100),
    author_flair_css character varying(100),
    edited boolean,
    body character varying(50000)
);
INSERT INTO reddit_subreddits (subreddit) VALUES ('subreddit');
