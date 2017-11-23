#!/usr/bin/env python
#
# Collect reddit comments in submissions created within a date range.
#
# Obtain the submissions made within a date range for a subreddit then walk the
# comment tree for those sumbissions to collect all comments.
#

import argparse
import configparser
from datetime import datetime, timedelta
import logging
import os
import sys
import time

import praw
import psycopg2

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('main')

working_dir = os.path.realpath(__file__)
working_dir = os.path.dirname(working_dir)

epoch = datetime.utcfromtimestamp(0)

def _connect_to_db(dh_host, db_name, db_user, db_user_pass):
    conn = None
    cursor = None

    connstr = \
            "host={db_host} " + \
            "dbname='{db_name}' " + \
            "user='{db_user}' " + \
            "password='{db_user_pass}'"
    connstr = connstr.format(
                db_host=db_host,
                db_name=db_name,
                db_user=db_user,
                db_user_pass=db_user_pass)
    try:
        conn = psycopg2.connect(connstr)

    except Exception as e:
        logger.exception(e)

    return conn

def parse_args():
    parser = argparse.ArgumentParser(
            description='Collect comments under submissions within a date range.')

    # TODO: Make subreddit a list with nargs+
    parser.add_argument('-r', '--subreddit', action='store', required=True,
            help='subreddit to collect')
    parser.add_argument('-p', '--period', nargs=2, action='store',
            required=True, help='start and end date range formatted yyyymmddhhmmss')
    parser.add_argument('-d', '--debug', action='store_true')

    args = parser.parse_args()

    # Order the start and end dates correctly regardless of how user specified them
    if args.period[0] > args.period[1]:
        _start_date = args.period[0]
        args.period[0] = args.period[1]
        args.period[1] = _start_date

    # Instantiate real datetime objects from command line variables
    args.period[0] = datetime.strptime(args.period[0], '%Y%m%d%H%M%S')
    args.period[1] = datetime.strptime(args.period[1], '%Y%m%d%H%M%S')

    return args

def parse_config():
    config = configparser.ConfigParser()
    config.read_file(open(os.path.join(working_dir, '../config.conf')))
    return config

def get_epoch(dt):
    return (dt - epoch).total_seconds()

def search_subreddit(subreddit, start_epoch, end_epoch):
    """Return a list of submissions between start and end epoch.

    Arguments:
        subreddit   - praw subreddit object
        start_epoch - float
        end_epoch   - float
    """

    query = 'timestamp:{start_epoch}..{end_epoch}'.format(
            start_epoch=int(start_epoch), end_epoch=int(end_epoch))
    submissions = subreddit.search(
            query,
            sort='new',
            syntax='cloudsearch')

    return list(submissions)

def get_genesis_post(subreddit, start_epoch, end_epoch, start_date, end_date):
    '''Walk subscriptions backwards; find the subreddit's first post.

    The origin submission for a subreddit would be the very first, still
    accessible, submission. Alternatively, this function returns the first
    submission within the specified date range.

    Arguments:
        subreddit   - PRAW object
        start_date  - datetime
        end_date    - datetime
    '''
    origin = None

    # Find the origin year
    while True:
        start_epoch = get_epoch(start_date)
        end_epoch = get_epoch(end_date)
        submissions = search_subreddit(subreddit, start_epoch, end_epoch)
        if len(submissions) == 0:
            end_date = end_date.replace(year=end_date.year + 1)
            break
        sys.stdout.write('\rsearching genesis post for {} in {}'.format(
                subreddit, end_date))
        sys.stdout.flush()
        end_date = end_date.replace(year=end_date.year - 1)

    # Reset end_epoch to last year
    end_epoch = get_epoch(end_date)

    # Now iterate months until we find the origin month
    while True:
        start_epoch = get_epoch(start_date)
        end_epoch = get_epoch(end_date)
        submissions = search_subreddit(subreddit, start_epoch, end_epoch)
        last_month = end_date
        if len(submissions) == 0:
            end_date = end_date
            break
        origin = submissions[-1].created
        sys.stdout.write('\rsearching genesis post for {} in {}'.format(
                subreddit, end_date))
        sys.stdout.flush()
        end_date = end_date.replace(day=end_date.day - end_date.day + 1)
        end_date += timedelta(days=-1)

    sys.stdout.write('\rfound {} origin submission at {}; epoch is {}\n'.format(
            subreddit, end_date, origin))
    sys.stdout.flush()
    return origin

def get_submission_objects(subreddit, start_epoch, end_epoch, end_date):
    """Collect all submissions in a range; return a list of submission objs."""
    i = 0
    ids = list()

    end_epoch_saved =  None
    while True:

        # We request all submissions within our start and end date range
        submissions = search_subreddit(subreddit, start_epoch, end_epoch)
        if len(submissions) == 0:
            sys.stdout.write('\n')
            sys.stdout.flush()
            break

        # A limited number of the newest submissions are returned
        #   so get the end_epoch of the last received submission and update our
        #   date range.
        end_epoch = submissions[-1].created - 1

        if i == 0:
            end_epoch_saved = end_epoch
        # Note: are we working with created, rather than created_utc? We might
        #   want to fix that sometime, but it works for now so who cares.
        end_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_epoch))

        # How many we've done / how manty to do
        now = get_epoch(datetime.now())
        percentage = (end_epoch_saved - end_epoch) / (end_epoch_saved - start_epoch) * 100
        sys.stdout.write(
                '\rharvest {} submission ids {:00.2f}% complete | '.format(subreddit, percentage) + \
                'date: {} end_epoc: {}'.format(end_date, end_epoch))
        sys.stdout.flush()

        for s in submissions:
            ids.append(s)

        i += 1

    return ids

def _get_last_n_submission_ids(table, n):
    """Get the last n submission ids from the database.

    Note:
    This function is in no way safe for public use!
    """

    cursor = None
    try:
        cursor = conn.cursor()

        sql = '''
            SELECT id
            FROM reddit_submissions
            ORDER BY created_utc DESC
            LIMIT {n};
        '''.format(table = table, n = n)
        cursor.execute(sql)
        rows = cursor.fetchall()

        ids = dict()
        for row in rows:
            ids[row[0]] = None

        return ids

    except Exception as e:
        logger.exception(e)

    finally:
        if cursor is not None:
            cursor.close()

if __name__ == '__main__':

    args = parse_args()
    config = parse_config()

    if args.debug:
        logger.level = logging.DEBUG

    db_host = config['DEFAULT']['db_host']
    db_name = config['DEFAULT']['db_name']
    db_user = config['DEFAULT']['db_user']
    db_pass = config['DEFAULT']['db_pass']

    logger.debug('instantiate reddit object')
    reddit = praw.Reddit(
            client_id = config['DEFAULT']['reddit_client_id'],
            client_secret = config['DEFAULT']['reddit_client_secret'],
            user_agent = config['DEFAULT']['reddit_user_agent'])

    global conn
    conn = None
    try:
        logger.debug('connect to database')
        conn = _connect_to_db(db_host, db_name, db_user, db_pass)
        if conn is None:
            sys.exit(1)

        subreddit = reddit.subreddit(args.subreddit)

        # Get datestuff in order
        start_date, end_date = args.period
        start_epoch = get_epoch(start_date)
        end_epoch = get_epoch(end_date)

        logger.debug('find genesis post')
        origin = get_genesis_post(subreddit, start_epoch, end_epoch, start_date, end_date)

        logger.debug('obtain submission ids within')
        submissions = get_submission_objects(subreddit, start_epoch, end_epoch, end_date)
        logger.info('collected {} submissions within range'.format(len(submissions)))

        cursor = conn.cursor()

        # TODO: get_last_n_submission_ids and avoid inserting duplicates

        for submission in submissions:
            sql = '''
                    INSERT INTO reddit_submissions (
                        id, subreddit, author, author_flair_text, author_flair_css,
                        created, created_utc, domain, downs, ups, score, num_comments, name,
                        permalink, url, selftext, title)
                    VALUES (%(id)s, %(subreddit)s, %(author)s, %(author_flair_text)s,
                        %(author_flair_css)s, %(created)s, %(created_utc)s, %(domain)s,
                        %(downs)s, %(ups)s, %(score)s, %(num_comments)s, %(name)s,
                        %(permalink)s, %(url)s, %(selftext)s, %(title)s);
            '''
            try:
                cursor.execute(sql,
                        {'id': submission.id,
                        'subreddit':submission.subreddit.display_name,
                        'author':str(submission.author).replace('\x00', ''),
                        'author_flair_text':submission.author_flair_text,
                        'author_flair_css':submission.author_flair_css_class,
                        'created':datetime.fromtimestamp(submission.created),
                        'created_utc':datetime.fromtimestamp(submission.created_utc),
                        'domain':submission.domain,
                        'downs':submission.downs,
                        'ups':submission.ups,
                        'score':submission.score,
                        'num_comments':submission.num_comments,
                        'name':submission.name,
                        'permalink':submission.permalink,
                        'url':submission.url,
                        'selftext':submission.selftext.replace('\x00', ''),
                        'title':submission.title}
                 )
            except Exception as e:
                logger.exception(e)
                logger.error(submission)
        conn.commit()

    except (KeyboardInterrupt, SystemExit):
        logger.info('exiting')

    except Exception as e:
        logger.exception(e)

    finally:
        if not conn is None:
            conn.close()

    sys.exit(0)
