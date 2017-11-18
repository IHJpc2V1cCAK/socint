#!/usr/bin/env python
#
# Collect a subreddit comment stream and log to database.
#
# Requirements are simple enough, just praw and psycopg2 for postgres access.
#   $ pip install praw psycopg2
#


import argparse
import configparser
import datetime
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

def parse_args():
    parser = argparse.ArgumentParser(
            description='Collect a subreddit stream to database.')

    parser.add_argument('-s', '--subreddit', action='store', required=True,
            help='subreddit to stream')
    parser.add_argument('-t', '--table', action='store',
            default='{subreddit}')
    parser.add_argument('-d', '--debug', action='store_true')

    args = parser.parse_args()
    args.table = args.table.format(subreddit=args.subreddit)
    return args

def parse_config():
    config = configparser.ConfigParser()
    config.read_file(open(os.path.join(working_dir, '../config.conf')))
    return config

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

def _get_last_n_ids(table, n):
    """Get the last n comment ids of a subreddit from our database.

    Arguments:
        table   - destination table, or source of comment id's in this case
        n       - number of comment ID's to pull
    Note:
    This function is in no way safe for public use!
    """

    cursor = None
    try:
        cursor = conn.cursor()

        sql = list()
        sql = '''
            SELECT id
            FROM {table}
            ORDER BY created_utc DESC
            LIMIT {n};
        '''.format(table=table, n=n)
        cursor.execute(sql)
        rows = cursor.fetchall()

        ids = list()
        for row in rows:
            ids.append(row[0])

        return ids

    except Exception as e:
        logger.exception(e)

    finally:
        if cursor is not None:
            cursor.close()


def _save_comment(subreddit, comment):
    """Save a comment to the database.

    Arguments:
        subreddit   - String of the subreddit USED IN CRAFTING OUR INSERT!
        comment     - A dictionary structure representing our table into which
                      we save the comment.

    Note:
    This function is NOT AT ALL SAFE for public use.
    """

    cursor = None
    try:
        cursor = conn.cursor()

        sql = list()
        sql = '''
                INSERT INTO {subreddit} (
                    id, parent_id, link_id, author, created, created_utc,
                    author_flair_text, author_flair_css, edited, body)
                VALUES (%(id)s, %(parent_id)s, %(link_id)s, %(author)s, %(created)s,
                    %(created_utc)s, %(author_flair_text)s, %(author_flair_css)s,
                    %(edited)s, %(body)s);
        '''.format(subreddit=subreddit)
        try:
            cursor.execute(sql, comment)
        except ValueError as e:
            logger.exception(e)
            logger.error('the comment is as follows\n')
            logger.error(comment)
        except Exception as e:
            logger.exception(e)

        conn.commit()
        return True

    except Exception as e:
        logger.exception(e)

    finally:
        if cursor is not None:
            cursor.close()


def collect_stream(subreddit_name, subreddit, ids):

    while True:
        try:
            i = 0
            skipped = 0
            for comment in subreddit.stream.comments(pause_after=-1):
                if comment is None:
                    continue
                if comment.id in ids:
                    skipped += 1
                    continue
                if skipped > 0:
                    logger.info('skipped {} comments; already logged'.format(skipped))
                    lds = ids[skipped:]
                    skipped = 0
                else:
                    if len(ids) > 0:
                        ids.pop(0)
                    ids.append(comment.id)

                comment_dict = {
                        'id': comment.id,
                        'parent_id': comment.parent_id,
                        'link_id': comment.link_id,
                        'author': str(comment.author).replace('\x00', ''),
                        'created': datetime.datetime.fromtimestamp(comment.created),
                        'created_utc': datetime.datetime.fromtimestamp(comment.created_utc),
                        'author_flair_text':comment.author_flair_text,
                        'author_flair_css':comment.author_flair_css_class,
                        'edited': bool(comment.edited),
                        'body': comment.body.replace('\x00', '')
                }
                _save_comment(
                        subreddit_name,
                        comment_dict)
                i += 1
                if i % 10 == 0:
                    dt = datetime.datetime.now()
                    msg = '\r{dt} logged {n} comments from {subreddit}'.format(
                            dt = str(dt), n = i, subreddit = args.subreddit)
                    sys.stdout.write(msg)
        except Exception as e:
            logger.exception(e)
            time.sleep(15)
        finally:
            pass

if __name__ == '__main__':

    args = parse_args()
    config = parse_config()

    if args.debug:
        logger.level = logging.DEBUG

    db_host = config['DEFAULT']['db_host']
    db_name = config['DEFAULT']['db_name']
    db_user = config['DEFAULT']['db_user']
    db_pass = config['DEFAULT']['db_pass']

    if not input('\nLog /r/{subreddit} to {table}? [y/N]'.format(
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


        # Get the last n comment ids so we don't try logging them a second time
        logger.debug('get last comments from database')
        ids = _get_last_n_ids(args.table, 500)

        logger.debug('instantiate subreddit object')
        subreddit = reddit.subreddit(args.subreddit)

        logger.debug('collect stream')
        collect_stream(args.subreddit, subreddit, ids)

    except (KeyboardInterrupt, SystemExit):
        logger.info('exiting')

    except Exception as e:
        logger.exception(e)

    finally:
        if not conn is None:
            conn.close()

    sys.exit(0)
