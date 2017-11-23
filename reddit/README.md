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


## Collect

### subreddit_stream.py

Attaches to the comment stream of a specified subreddit and logs most comments real-time.

### subreddit_comments.py

Accumulates all submissions within a specified date range then collects all comments within those submissions.

```
(env)~/socint/reddit/collect$ ./subreddit_comments.py \
        -s example_subreddit \
        -d 20170101000000 20171231235959
```

### redditor_history.py

Collects up to the last 1000 comments of a redditor. By default these comments are stored to a table named user_comments instead of to tables by the same name as the subreddit.

### subreddit_submissions.py

Log submissions to a subreddit for a period of time. Information such as the submission's title, body if it's a self-post, url if it's a link submission, author, vote scores and more.

```
(env)~/socint/reddit/collect$ ./subreddit_submissions.py \
        -r example_subreddit \
        -p 20170101000000 20171231235959
```


## Report

## user_schedule.py

Graphs a user's post schedule and outputs other information such as which
subreddits they participate in most.

```
(env)~/socint/reddit/report/user_schedule.py -u spez
INFO:main:querying user schedule
INFO:main:querying user_comments
INFO:main:generate schedule graph
INFO:main:obtaining user history

/u/spez history has 966 posts
 announcements         475 (49.2%)
 reddit.com             76 ( 7.9%)
 IAmA                   52 ( 5.4%)
 cscareerquestions      47 ( 4.9%)
 programming            34 ( 3.5%)
 modnews                34 ( 3.5%)
 technology             26 ( 2.7%)
 [...snip...]
```
