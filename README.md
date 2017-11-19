# Social Intelligence

Utilities & scripts to collect and find insight from social network data.

Store a local cache of data from social media sites in a Postgres database for
analysis. Report on interesting trends, user patterns, etc..

## Quick Example

Lets see what a user's been doing lately:

```
(env)~/reddit$ ./user_stats
INFO:main:obtaining user history: spez
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
![spez posting schedule](https://raw.githubusercontent.com/IHJpc2V1cCAK/socint/master/doc/reddit_user_schedule_spez.png)


## Get Started

1. Clone the repo
2. Get a Postgres database running
3. Dive into the platform-specific directories (e.g. [Reddit](https://github.com/IHJpc2V1cCAK/socint/tree/master/reddit), or [Twitter](https://github.com/IHJpc2V1cCAK/socint/tree/master/twitter))
4. Follow platform-specific instructions found in that README
5. Run some scripts under ./collect ... then check out what's available under ./report

## Where are we going with this?

There's a lot of messy code that should be cleaned up and uploaded as this
collection progresses. Check back soon.
