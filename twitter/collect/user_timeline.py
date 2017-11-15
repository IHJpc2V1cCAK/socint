#!/usr/bin/env python
#

import argparse
import configparser
import csv
import datetime
import logging
import sys

import tweepy


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('main')

def parse_args():
    parser = argparse.ArgumentParser(
            description='Collect a users comment history.')

    parser.add_argument('-u', '--user', action='store', required=True,
            help='redditor to collect')
    parser.add_argument('-o', '--output', action='store', default='./output.csv')
    parser.add_argument('-d', '--debug', action='store_true')

    args = parser.parse_args()
    return args

def parse_config():
    config = configparser.ConfigParser()
    config.read_file(open('./config.conf'))
    return config

if __name__ == '__main__':
    config = parse_config()
    twitter_consumer_key = config['DEFAULT']['twitter_consumer_key']
    twitter_consumer_secret = config['DEFAULT']['twitter_consumer_secret']
    access_token = config['DEFAULT']['twitter_access_token']
    access_secret = config['DEFAULT']['twitter_access_token_secret']

    args = parse_args()

    auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
    auth.set_access_token(access_token, access_secret)

    api = tweepy.API(auth)
    #public_tweets = api.user_timeline()

    with open(args.output, 'w') as csvfile:
        field_names = ['author.screen_name', 'created_at_utc', 'lang',
            'favorite_count', 'retweet_count', 'retweet_status.author.name',
            'hashtags', 'urls', 'text']
        csv_writer = csv.DictWriter(
                csvfile, delimiter=',',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                fieldnames=field_names)
        csv_writer.writeheader()

        # TODO: Clean up this awful super-nesting
        for page in tweepy.Cursor(api.user_timeline, id=args.user).pages():

            for tweet in page:
                # There's a ton of other stuff available. Dump the full contents of a
                #   tweet from the api using pprint.pprint(vars(tweet))
                if tweet.retweeted:
                    retweet_status_author_name = tweet.retweeted_status.author.name
                else:
                    retweet_status_author_name = None
                hashtags = []
                for hashtag in tweet.entities['hashtags']:
                    hashtags.append(hashtag['text'])
                urls = []
                for url in tweet.entities['urls']:
                    urls.append(url['expanded_url'])
                t = tweet.text.encode('utf-8')
                csv_writer.writerow({
                        'author.screen_name': tweet.author.screen_name,
                        'created_at_utc': (tweet.created_at - \
                                datetime.datetime.utcfromtimestamp(0)).total_seconds(),
                        'lang': tweet.lang,
                        'favorite_count': tweet.favorite_count,
                        'retweet_count': tweet.retweet_count,
                        'retweet_status.author.name': retweet_status_author_name,
                        'hashtags': ' '.join(hashtags),
                        'urls': ' '.join(urls),
                        'text': tweet.text})
