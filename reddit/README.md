# Reddit

Reddit specific scripts & utilities.

All scripts revolve around collection & storage of comments. Other Reddit objects might be added in the future. You'll need at least one table to which comments are logged. The table structure these scripts expect is documented in `./sql/schema/comment_table.sql` and by default scripts log comments of a subreddit into a table by the same name.

To get started:
```
# Create and edit the config file.
~/socint/reddit/collect$ cp ./config.conf.sample ./conf.conf
~/socint/reddit/collect$ gedit ./conf.conf

# Create a virtual environment and install requirements
~/socint/reddit/collect$ virtualenv --python=/usr/bin/python3 ./env
~/socint/reddit/collect$ . ./env/bin/activate
(env)~/socint/twitter/collect$ pip install -r ./requirements.txt
    ...
    ...
```

## subreddit_stream.py

Attaches to the comment stream of a specified subreddit and logs most comments real-time.

## subreddit_comments.py

Accumulates all submissions within a specified date range then collects all comments within those submissions.

```
(env)~/socint/reddit/collect$ ./subreddit_comments.py \
        -s example_subreddit \
        -d 20170101000000 20171231235959
```

## redditor_history.py

Collects up to the last 1000 comments of a redditor. By default these comments are stored to a table named user_comments instead of to tables by the same name as the subreddit.
