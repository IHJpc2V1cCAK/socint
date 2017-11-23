-- holds submissions
CREATE TABLE reddit_submissions (
    id character varying(15) NOT NULL,
    subreddit character varying(50),
    author character varying(20),
    author_flair_text character varying(100),
    author_flair_css character varying(100),
    created timestamp without time zone,
    created_utc timestamp without time zone,
    domain character varying(1000),
    downs integer,
    ups integer,
    score integer,
    num_comments integer,
    name character varying(15),
    permalink character varying(100),
    url character varying(1000),
    selftext character varying(40000),
    title character varying(1000)
);
