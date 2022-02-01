import json
import re
import dask.bag as db
from dask.diagnostics import ProgressBar
from config_file import TWEETS_DATA_PATH, WORD_COUNT_PATH


def remove_punctuation(x):
    ''' Remove punctuation from str using Regex

    args:
        x (str): String to be processed

    returns:
        str
    '''
    return re.sub(r'[^\w\s]', '', x)


def extract_tweets(target_path):
    ''' Extract records from JSON files into Dask data bag

    args:
        target_path (str): File path for Tweets directory

    returns:
        Dask data bag
    '''
    return db.read_text(target_path).map(json.loads)


def flatten_records(record):
    ''' Flatten data bag record and keep relevant attributes 

    args:
        record (object): Dask data bag record

    returns:
        Dask data bag record
    '''
    return {
        'id': record['id'],
        'text': record['text'],
        'user_id': record['user']['id'],
        'user_time_zone': record['user']['time_zone'],
        'retweet_count': record['retweet_count'],
        'favorite_count': record['retweet_count'],
        'lang': record['lang']
    }


def process_word_counts(records):
    ''' Pre-process text records and compute word frequencies

    args:
        records (dask.bag): Dask data bag

    returns:
        Dask data bag
    '''
    text = records.pluck('text')

    # pre-process records - removing punctuation and converting to lowercase
    words = text \
        .map(remove_punctuation) \
        .map(str.lower) \
        .str.split(" ") \
        .flatten() \
        .filter(lambda x: x != '')

    return words.frequencies(sort=True)


def load_word_counts_to_parquet(records, output_path):
    ''' Load word counts to Parquet file

    args:
        records (dask.bag): Dask data bag
        output_path (str): Path of output Parquet file

    returns:
        None
    '''
    with ProgressBar():
        df = records.to_dataframe().compute()
        df.columns = ['word', 'frequency']
    df.to_parquet(output_path, compression='gzip')


def load_tweet_records_to_store(records):
    # store flattened tweet records to store for future segmentation queries
    # will need to link with Tweet ID from Search Index during querying
    pass


def main():
    tweet_bag = extract_tweets(TWEETS_DATA_PATH)

    tweet_records = tweet_bag.filter(
        lambda x: 'text' in x).map(flatten_records)
    load_tweet_records_to_store(tweet_records)

    word_counts = process_word_counts(tweet_records)
    load_word_counts_to_parquet(word_counts, WORD_COUNT_PATH)


if __name__ == "__main__":
    main()
