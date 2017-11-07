# Reddit

Reddit specific scripts & utilities.

All scripts revolve around collection & storage of comments. Other Reddit objects might be added in the future. You'll need at least one table to which comments are logged. The table structure these scripts expect is documented in `./sql/schema/comment_table.sql` and by default scripts log comments of a subreddit into a table by the same name.

## subreddit_stream.py

Attaches to the comment stream of a specified subreddit and logs most comments real-time.

## subreddit_comments.py

Accumulates all submissions within a specified date range then collects all comments within those submissions.

## redditor_history.py

Collects up to the last 1000 comments of a redditor. By default these comments are stored to a table named user_comments instead of to tables by the same name as the subreddit.
