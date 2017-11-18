#!/usr/bin/env python
#

import argparse
import configparser
import logging
import operator
import os
import sys
import time

from bokeh.io import output_file, save
from bokeh.layouts import row, column
from bokeh.plotting import figure
from bokeh.models import FuncTickFormatter, HoverTool
import psycopg2

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('main')

working_dir = os.path.realpath(__file__)
working_dir = os.path.dirname(working_dir)

def parse_args():
    parser = argparse.ArgumentParser(
            description='Generate a user post schedule showing when they use Reddit.')

    parser.add_argument('-u', '--user', action='store', required=True,
            help='User to inspect')
    parser.add_argument('-d', '--debug', action='store_true')

    args = parser.parse_args()
    args.user = args.user.lower()
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
        print('\nCould not connect to the database: {}'.format(e))

    return conn

def _user_weekly(user):
    """Query tables to get a user posting schedule grouped by day and hour.

    The user_comments table, then subreddit tables are queried in succession,
    grouped by day of week (Mon through Sun) and hour of day to produce a
    user posting schedule.
    """
    # TODO: Add an argument to specify additional tables, default ALL
    #   which could pull from a "metatable" in postgres listing tables we've
    #   scraped.

    # This query only works on the user_comments table
    with open(os.path.join(working_dir, 'sql/query/user_weekly.sql'), 'r') as fin:
        sql = fin.read()

    logger.info('querying {}'.format('user_comments'))
    sql = sql.format(user = user, table='user_comments')
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    cursor = conn.cursor()
    cursor.execute('SELECT subreddit FROM reddit_subreddits;')
    subreddits = cursor.fetchall()
    for subreddit in subreddits:
        # This query excludes comments that are part of the user post history
        # pulled previously.
        with open(os.path.join(
                working_dir, 'sql/query/user_weekly_other.sql'), 'r') as fin:
            sql = fin.read()

        logger.info('querying {}'.format(subreddit[0]))
        sql = sql.format(user = user, table=subreddit[0])
        cursor = conn.cursor()
        cursor.execute(sql)
        other_rows = cursor.fetchall()
        i = 0
        final_rows = list()
        # Add each row from both tuples together. This is a mess, but it works.
        for row in other_rows:
            day = (row[0], )
            counts = tuple(map(operator.add, rows[i][1:], row[1:]))
            final_rows.append(day + counts)
            i += 1

        rows = final_rows

    return rows

def _generate_users_weekly_graph(users,append_report_name):
    """Generate a chart showing days & times users comment most frequently.

       users    - a dictionary of users & their data {'user_name':rows}
       append_report_name   - help name the report appropriately
    """

    output_file(os.path.join(
            working_dir, './output/users_{append_report_name}.html'.format(
                    append_report_name = append_report_name)))

    time_24h = [i for i in range(1,25)]

    plots = list()
    for user, rows in users.items():
        p = figure(y_range=(0,8), x_range=(0,25),
                width=800,
                height=500,
                title='/u/{user} posting schedule'.format(user = user),
                y_axis_label='Day',
                x_axis_label='Hour ({tz})'.format(tz = \
                        time.tzname[0] + '/' + time.tzname[1]))
                        # ^^^ assumes your sql server is set to same timezone
                        # as the computer running this script
                        # TODO: allow orientation to time zone by modifying
                        #   the SQL queries w/ "AT TIME ZONE 'UTC'"

        for i, row in enumerate(rows, start=1):
            x = [i]*24
            p.circle(time_24h, x, size=row[1:], fill_alpha=0.6)

        labels = {
                0:'',
                1:'Sunday',
                2:'Monday',
                3:'Tues',
                4:'Wed',
                5:'Thu',
                6:'Fri',
                7:'Sat',
                8:''
        }
        p.yaxis.formatter = FuncTickFormatter(code="""
            var labels = {};
            return labels[tick];
        """.format(labels))

        plots.append(p)

    document = column(plots)
    save(document)

def _get_user_history(user):
    cursor = conn.cursor()
    cursor.execute('''
            select subreddit,
                count(id) as post_count
            from user_comments
            where lower(author) = %(user)s
            group by subreddit
            order by post_count desc;
    ''', {'user':user})
    rows = cursor.fetchall()
    return rows

def _get_oldest_and_newest_comment(user):
    """Get the oldest known and most recent comment dates."""
    # TODO: Get additional stats.
    #   * select count group by year, month, day, hour and sort -- get post rate
    #   * Longest period of time between posts?
    #   * another idea I forgot
    cursor = conn.cursor()
    cursor.execute('''
            select created_utc
            from user_comments
            where lower(author) = %(user)s
            order by created_utc
            limit 1;
    ''', {'user': user})
    rows = cursor.fetchall()
    oldest = rows[0][0]

    cursor.execute('''
            select created_utc
            from user_comments
            where lower(author) = %(user)s
            order by created_utc DESC
            limit 1;
    ''', {'user': user})
    rows = cursor.fetchall()
    newest = rows[0][0]

    return (newest, oldest,)

def _get_linear_usage_history(start_date, end_date):
    pass

def _user_history_count(rows):
    c = 0
    for row in rows:
        c += row[1]
    return c

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

    try:
        logger.debug('connect to database')
        conn = _connect_to_db(db_host, db_name, db_user, db_pass)
        if conn is None:
            sys.exit(1)

        users = dict()
        logger.info('aggregate user schedule')
        users = {args.user: _user_weekly(args.user)}
        logger.info('graphing schedule')
        _generate_users_weekly_graph(users, args.user)
        # TODO: linear usage graph

        logger.info('obtaining user history')
        rows = _get_user_history(args.user)

        #logger.info('get oldest known comment')
        #print(_get_oldest_and_newest_comment(args.user))

        count = _user_history_count(rows)
        print('\n')
        print('/u/{u} history has {c} posts'.format(u = args.user, c = count))
        longest_subreddit = 0
        for row in rows:
            if len(row[0]) > longest_subreddit:
                longest_subreddit = len(row[0])
        for row in rows:
            subreddit = ' ' + row[0]
            subreddit += ' ' * (longest_subreddit - len(row[0]))
            subreddit += ' {: >5} '.format(row[1])
            subreddit += '({:#5.1%})'.format((row[1] / count))

            print(subreddit)
        print('\n')

    except (KeyboardInterrupt, SystemExit):
        logger.info('exiting')

    except Exception as e:
        logger.exception(e)

    finally:
        if not conn is None:
            conn.close()

    sys.exit(0)
