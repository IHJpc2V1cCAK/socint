#!/usr/bin/env python
#
# Generates comment count bar graph for one subreddit and stacks multi comments.
#
# The bar graph stacks comment counts on top of one another. This graph shows
# the which of multiple comments was used more frequently.
#

import argparse
import configparser
from datetime import datetime
import logging
import math
import os
import sys

from bokeh.core.properties import value
from bokeh.io import output_file, save
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
import pandas as pd
import psycopg2

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('main')

working_dir = os.path.realpath(__file__)
working_dir = os.path.dirname(working_dir)

# Good collection of colors that are very distinguished from each other
# Maybe share further up the project directory at some point
# Found on http://there4.io/2012/05/02/google-chart-color-list/
color_contrast = [
        '#3366CC'
        ,'#DC3912'
        ,'#FF9900'
        ,'#109618'
        ,'#990099'
        ,'#3B3EAC'
        ,'#0099C6'
        ,'#DD4477'
        ,'#66AA00'
        ,'#B82E2E'
        ,'#316395'
        ,'#994499'
        ,'#22AA99'
        ,'#AAAA11'
        ,'#6633CC'
        ,'#E67300'
        ,'#8B0707'
        ,'#329262'
        ,'#5574A6'
        ,'#3B3EAC'
]

def parse_args():
    parser = argparse.ArgumentParser(
            description='Generate a user post schedule showing when they use Reddit.')

    parser.add_argument('-g', '--groupby', '--group', action='store',
            choices=['hour', 'day', 'week'],
            required=True, help='group comment counts into hours, days, or weeks')
    parser.add_argument('-o', '--output', action='store',
            required=False, help='output file',
            default='./output/grouped_bar_graph_temp_name.html')
    parser.add_argument('-p', '--period', nargs=2, action='store',
            required=False, help='start and end date range formatted yyyymmddhhmmss')
    parser.add_argument('-r', '--subreddit', action='store', required=True,
            help='the subreddits to process')
    parser.add_argument('-t', '--terms', action='store', required=True,
            nargs='+', help='list of search terms')
    parser.add_argument('-v', '--verbose', action='store_true')

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

def _get_subreddit_postcount(subreddit, term):
    """Query and return a dataframe for a subreddit and terms list."""

    # Build the where clause
    params = dict()

    # TODO: Add a negation clause. Terms can be prepended with a minus to
    #   eliminate counting comments containing those terms.
    # TODO: Add a sql parameter to override our query generation with a custom
    #   query if you need more refined filtering that combines terms using
    #   AND with OR.

    and_clause = "lower(body) like %(term)s"
    params['term'] = term
    term = term.replace('%', '')
    term = term.replace('\'', '').strip()

    if args.groupby == 'week':
        sql = '''
                SELECT
                    '{term}' as term,
                    to_char(EXTRACT(YEAR from superdatetime.base), 'FM0000')||to_char(EXTRACT(WEEK from superdatetime.base), 'FM00') as period,
                    count(DISTINCT s.id)
                FROM superdatetime
                    LEFT JOIN {subreddit} s ON to_char(EXTRACT(YEAR from s.created_utc), 'FM0000')||to_char(EXTRACT(WEEK from s.created_utc), 'FM00') =
                            to_char(EXTRACT(YEAR from superdatetime.base), 'FM0000')||to_char(EXTRACT(WEEK from superdatetime.base), 'FM00')
                        AND ({and_clause})
                WHERE superdatetime.base between %(start_date)s and %(end_date)s
                GROUP BY to_char(EXTRACT(YEAR from superdatetime.base), 'FM0000')||to_char(EXTRACT(WEEK from superdatetime.base), 'FM00');
        '''.format(term = term, subreddit = subreddit, and_clause = and_clause)
    elif args.groupby == 'day':
        sql = '''
                SELECT
                    '{term}' as term,
                    to_char(EXTRACT(YEAR from superdatetime.base), 'FM0000')||to_char(EXTRACT(MONTH from superdatetime.base), 'FM00')||to_char(EXTRACT(DAY from superdatetime.base), 'FM00') as period,
                    count(DISTINCT s.id)
                FROM superdatetime
                    LEFT JOIN {subreddit} s ON to_char(EXTRACT(YEAR from s.created_utc), 'FM0000')||to_char(EXTRACT(MONTH from s.created_utc), 'FM00')||to_char(EXTRACT(DAY from s.created_utc), 'FM00') =
                            to_char(EXTRACT(YEAR from superdatetime.base), 'FM0000')||to_char(EXTRACT(MONTH from superdatetime.base), 'FM00')||to_char(EXTRACT(DAY from superdatetime.base), 'FM00')
                        AND ({and_clause})
                WHERE superdatetime.base between %(start_date)s and %(end_date)s
                GROUP BY to_char(EXTRACT(YEAR from superdatetime.base), 'FM0000')||to_char(EXTRACT(MONTH from superdatetime.base), 'FM00')||to_char(EXTRACT(DAY from superdatetime.base), 'FM00');
        '''.format(term = term, subreddit = subreddit, and_clause = and_clause)
    elif args.groupby == 'hour':
        sql = '''
                SELECT
                    '{subreddit}' as subreddit,
                    to_char(EXTRACT(YEAR from superdatetime.base), 'FM0000')||to_char(EXTRACT(MONTH from superdatetime.base), 'FM00')||to_char(EXTRACT(DAY from superdatetime.base), 'FM00')||to_char(EXTRACT(HOUR from superdatetime.base), 'FM00') as period,
                    count(DISTINCT s.id)
                FROM superdatetime
                    LEFT JOIN {subreddit} s ON to_char(EXTRACT(YEAR from s.created_utc), 'FM0000')||to_char(EXTRACT(MONTH from s.created_utc), 'FM00')||to_char(EXTRACT(DAY from s.created_utc), 'FM00')||to_char(EXTRACT(HOUR from s.created_utc), 'FM00') =
                            to_char(EXTRACT(YEAR from superdatetime.base), 'FM0000')||to_char(EXTRACT(MONTH from superdatetime.base), 'FM00')||to_char(EXTRACT(DAY from superdatetime.base), 'FM00')||to_char(EXTRACT(HOUR from superdatetime.base), 'FM00')
                        AND ({and_clause})
                WHERE superdatetime.base between %(start_date)s and %(end_date)s
                GROUP BY to_char(EXTRACT(YEAR from superdatetime.base), 'FM0000')||to_char(EXTRACT(MONTH from superdatetime.base), 'FM00')||to_char(EXTRACT(DAY from superdatetime.base), 'FM00')||to_char(EXTRACT(HOUR from superdatetime.base), 'FM00');
        '''.format(term = term, subreddit = subreddit, and_clause = and_clause)

    logger.debug(sql)
    logger.debug(params)
    start_date, end_date = args.period
    params['start_date'] = start_date.isoformat()
    params['end_date'] = end_date.isoformat()
    logger.debug(params)
    df = pd.read_sql_query(
            sql = sql,
            con = conn,
            params = params
    )
    return df

def _gen_graph(df, subreddit):

    # sorry, this function is ugly. It's ripped out of bokeh docs, mangled to
    #   work for our purposes. A lot of the other glyphs (?) accept a
    #   dataframe much more cleanly & directly.
    i = 0
    terms = list()
    colors = list()
    for t in df['term']:
        if not t in terms:
            terms.append(t)
            colors.append(color_contrast[i])
            i += 1

    periods = list()
    for p in df['period']:
        if not str(p) in periods:
            periods.append(str(p))

    logger.info('generating graph')
    output_file(os.path.join(working_dir, args.output))

    df = df.pivot(index='term', columns='period', values='count')
    data = {'periods' : periods}
    for term in terms:
        data[term] = [x for x in df.loc[term,:]]
    source = ColumnDataSource(data=data)

    title = 'Stacked count of comments in ' + \
            '"/r/{subreddit}" grouped by {group}'.format(
                    group=args.groupby,
                    subreddit = subreddit)
    p = figure(x_range=periods, plot_height=350, title=title,
               toolbar_location=None, tools="", width=800)
    p.xaxis.major_label_orientation = math.pi/4

    p.vbar_stack(terms, x='periods', width=0.9, color=colors, source=source,
            legend=[value(x) for x in terms])

    p.legend.location = 'top_left'
    save(p)

if __name__ == '__main__':

    args = parse_args()
    config = parse_config()

    if args.verbose:
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

        df = pd.DataFrame()
        for term in args.terms:
            logger.info('querying {subreddit} for {term}'.format(
                    subreddit=args.subreddit, term=term))
            df = df.append(_get_subreddit_postcount(args.subreddit, term))

        _gen_graph(df, args.subreddit)

    except (KeyboardInterrupt, SystemExit):
        logger.info('exiting')

    except Exception as e:
        logger.exception(e)

    finally:
        if not conn is None:
            conn.close()

    sys.exit(0)
