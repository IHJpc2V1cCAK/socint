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
import sys
import time

import praw
import psycopg2

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('main')

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

    parser.add_argument('-s', '--subreddit', action='store', required=True,
            help='subreddit to collect')
    parser.add_argument('-r', '--daterange', nargs=2, action='store',
            required=True, help='start and end date range formatted yyyymmddhhmmss')
    parser.add_argument('-t', '--table', action='store',
            default='{subreddit}')
    parser.add_argument('-u', '--unsafe', action='store_true',
            help='do not pull a list of comment ids already in the destination table')
    parser.add_argument('-d', '--debug', action='store_true')

    args = parser.parse_args()

    # Our default table has a variable to fill in to make a complete table name
    args.table = args.table.format(subreddit=args.subreddit)

    # Order the start and end dates correctly regardless of how user specified them
    if args.daterange[0] > args.daterange[1]:
        _start_date = args.daterange[0]
        args.daterange[0] = args.daterange[1]
        args.daterange[1] = _start_date

    # Instantiate real datetime objects from command line variables
    args.daterange[0] = datetime.strptime(args.daterange[0], '%Y%m%d%H%M%S')
    args.daterange[1] = datetime.strptime(args.daterange[1], '%Y%m%d%H%M%S')

    return args

def parse_config():
    config = configparser.ConfigParser()
    config.read_file(open('./config.conf'))
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

def get_submission_ids(subreddit, start_epoch, end_epoch, end_date):
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
            ids.append(s.id)

        i += 1

    return ids

def _get_last_n_comment_ids(table, n):
    """Get the last n comment ids of a subreddit from our database.

    Note:
    This function is in no way safe for public use!
    """

    cursor = None
    try:
        cursor = conn.cursor()

        sql = '''
            SELECT id
            FROM {table}
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
        print('\nan error occurred obtaining existing ids: {}'.format(e))

    finally:
        if cursor is not None:
            cursor.close()

def _dump_to_database(full_comments, table):
    """Given a list of comments (in dictionary form) dump them to the database.

    safe or unsafe, pulls a list of existing comment ID's from the destination
    table and skips comments based on what's already there.

    Returns inserted_comments, skipped_comments, failed_comments
    """

    cursor = None
    try:

        existing_ids = _get_last_n_comment_ids(table, 10000000)
        skipped_comments = 0
        new_comments = list()
        for c in full_comments:
            if c['id'] in existing_ids:
                skipped_comments += 1
                continue
            new_comments.append(c)

        cursor = conn.cursor()

        sql = list()
        inserted_comments = 0
        failed_comments = 0
        sql = None
        for comment in new_comments:
            sql = '''
                    INSERT INTO {table} (
                        id, parent_id, link_id, author, created, created_utc,
                        author_flair_text, author_flair_css, edited, body)
                    VALUES (%(id)s, %(parent_id)s, %(link_id)s, %(author)s, %(created)s,
                        %(created_utc)s, %(author_flair_text)s, %(author_flair_css)s,
                        %(edited)s, %(body)s);
            '''.format(table=table)
            try:
                cursor.execute(sql, comment)
                inserted_comments += 1
            except Exception as e:
                logger.exception(e)
                logger.error(comment)
                failed_comments += 1

        conn.commit()
        return inserted_comments, skipped_comments, failed_comments

    except Exception as e:
        logger.exception(e)

    finally:
        if cursor is not None:
            cursor.close()

def collect_comments(subreddit_name, ids, destination_table):
    try:
        full_comments = list()
        i = 0
        for s_id in ids:
            i += 1
            submission = praw.models.Submission(reddit, id=s_id)
            submission.comments.replace_more(limit=0)
            comments = submission.comments.list()
            for comment in comments:
                full_comments.append({
                        'id': comment.id,
                        'parent_id': comment.parent_id,
                        'link_id': comment.link_id,
                        'author': str(comment.author).replace('\x00', ''),
                        'created': datetime.fromtimestamp(comment.created),
                        'created_utc': datetime.fromtimestamp(comment.created_utc),
                        'author_flair_text':comment.author_flair_text,
                        'author_flair_css':comment.author_flair_css_class,
                        'edited': bool(comment.edited),
                        'body': comment.body.replace('\x00', '')
                })

            percentage = float(i) / float(len(ids)) * 100
            msg = '\rProcessing {} of {} submissions in {}: {:00.5f}%'
            sys.stdout.write(msg.format(i, len(ids), subreddit_name, percentage))
            sys.stdout.flush()

            if i % 1000 == 0:
                ##############################################################
                ### This stuff's all over the place, should probably go into a
                ### intermediary function of it's own for convenience
                ##############################################################
                sys.stdout.write(' [...]')
                sys.stdout.flush()
                # get a fresh collection if ids alreadyin the database
                time.sleep(5)
                inserted_comments, skipped_comments, failed_comments = \
                        _dump_to_database(full_comments, destination_table)
                sys.stdout.write(' {} records written, {} skipped, {} failed; (last id {}) \n'.format(
                        inserted_comments,
                        skipped_comments,
                        failed_comments,
                        s_id.strip()))
                sys.stdout.flush()
                # Reset our comment list
                full_comments = list()

        sys.stdout.write(' [...]')
        sys.stdout.flush()
        # get a fresh collection if ids already in the database
        time.sleep(5)
        inserted_comments, skipped_comments, failed_comments = \
                _dump_to_database(full_comments, destination_table)
        sys.stdout.write(' {} records written, {} skipped, {} failed; (last id {}) \n'.format(
                inserted_comments,
                skipped_comments,
                failed_comments,
                s_id.strip()))
        sys.stdout.flush()

    except (KeyboardInterrupt, SystemExit):
        sys.stdout.write(' [...]')
        sys.stdout.flush()
        # get a fresh collection if ids alreadyin the database
        time.sleep(5)
        inserted_comments, skipped_comments, failed_comments = \
                _dump_to_database(full_comments, destination_table)
        sys.stdout.write(' {} records written, {} skipped, {} failed; (last id {}) \n'.format(
                inserted_comments,
                skipped_comments,
                failed_comments,
                s_id.strip()))
        sys.stdout.flush()

    except Exception as e:
        logger.exception(e)

if __name__ == '__main__':

    args = parse_args()
    config = parse_config()

    if args.debug:
        logger.level = logging.DEBUG

    db_host = config['DEFAULT']['db_host']
    db_name = config['DEFAULT']['db_name']
    db_user = config['DEFAULT']['db_user']
    db_pass = config['DEFAULT']['db_pass']

    if not input('\nLog /r/{subreddit} comments to {table}? [y/N]'.format(
            subreddit=args.subreddit, table=args.table)).lower() == 'y':
        sys.exit(0)

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
        start_date, end_date = args.daterange
        start_epoch = get_epoch(start_date)
        end_epoch = get_epoch(end_date)

        logger.debug('find genesis post')
        origin = get_genesis_post(subreddit, start_epoch, end_epoch, start_date, end_date)

        logger.debug('obtain submission ids within')
        ids = get_submission_ids(subreddit, start_epoch, end_epoch, end_date)
        logger.info('collected {} submissions within range'.format(len(ids)))

        collect_comments(args.subreddit, ids, args.table)

    except (KeyboardInterrupt, SystemExit):
        logger.info('exiting')

    except Exception as e:
        logger.exception(e)

    finally:
        if not conn is None:
            conn.close()

    sys.exit(0)
