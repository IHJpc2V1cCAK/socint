#!/usr/bin/env python
#
# Given a redditor's name, obtain a list of their latest comments (up to
# 1000 of them) and save them to the database.
#
# The default table to which user comments are stored is "user_comments".

import argparse
import configparser
import datetime
import logging
import os
import sys

import praw
import psycopg2


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('main')

working_dir = os.path.realpath(__file__)
working_dir = os.path.dirname(working_dir)

def parse_args():
    parser = argparse.ArgumentParser(
            description='Collect a users comment history.')

    parser.add_argument('-u', '--user', action='store', required=True,
            help='redditor to collect')
    parser.add_argument('-t', '--table', action='store',
            default='user_comments')
    parser.add_argument('-d', '--debug', action='store_true')

    args = parser.parse_args()
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
        logger.error('\nCould not connect to the database: {}'.format(e))

    return conn

def _save_comments(comments):
    """given a ListingGenerator of reddit comments, save to the database."""

    cursor = None
    try:
        cursor = conn.cursor()
        for comment in comments:
            sql = '''
                    INSERT INTO user_comments (
                        id, author, subreddit, created, created_utc,
                        author_flair_text, author_flair_css, link_permalink,
                        body)
                    VALUES (%(id)s, %(author)s, %(subreddit)s, %(created)s,
                        %(created_utc)s, %(author_flair_text)s,
                        %(author_flair_css)s, %(link_permalink)s, %(body)s)
            '''

            args = {
                    'id':comment.id,
                    'author':str(comment.author).replace('\x00', ''),
                    'subreddit':str(comment.subreddit),
                    'created':datetime.datetime.fromtimestamp(comment.created),
                    'created_utc':datetime.datetime.fromtimestamp(comment.created_utc),
                    'author_flair_text':comment.author_flair_text,
                    'author_flair_css':comment.author_flair_css_class,
                    'link_permalink':comment.link_permalink,
                    'body':comment.body.replace('\x00', '')
            }

            cursor.execute(sql, args)

        conn.commit()

    except Exception as e:
        logger.exception(e)
    finally:
        if not cursor is None:
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

    global conn
    conn = None

    logger.debug('connect to database')
    conn = _connect_to_db(db_host, db_name, db_user, db_pass)
    if conn is None:
        sys.exit(1)

    logger.debug('instantiate reddit object')
    reddit = praw.Reddit(
            client_id = config['DEFAULT']['reddit_client_id'],
            client_secret = config['DEFAULT']['reddit_client_secret'],
            user_agent = config['DEFAULT']['reddit_user_agent'])

    try:

        logger.info('obtaining user history: {}'.format(args.user))
        comments = reddit.redditor(args.user).comments.new(limit=None)
        _save_comments(comments)

    except (KeyboardInterrupt, SystemExit):
        logger.info('exiting')

    except Exception as e:
        logger.exception(e)

    finally:
        if not conn is None:
            conn.close()

    sys.exit(0)
