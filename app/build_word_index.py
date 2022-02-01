import os
import json
import dask.bag as db
from whoosh import index
from whoosh.fields import Schema, TEXT, ID, NUMERIC
from config_file import SEARCH_INDEX_DIR, FIRST_FILE_PATH

schema = Schema(id=ID(stored=True), text=TEXT(
    stored=True), retweet_count=NUMERIC(stored=True), favorite_count=NUMERIC(stored=True))


def build_search_index(df):
    if not os.path.exists(SEARCH_INDEX_DIR):
        os.mkdir(SEARCH_INDEX_DIR)

    ix = index.create_in(SEARCH_INDEX_DIR, schema)
    writer = ix.writer()

    for i in df.index:
        writer.add_document(id=str(df.loc[i, "id"]), text=df.loc[i, "text"],
                            retweet_count=df.loc[i, "retweet_count"], favorite_count=df.loc[i, "favorite_count"])
    writer.commit()


def text_flatten(record):
    return {
        'id': record['id'],
        'text': record['text'],
        'retweet_count': record['retweet_count'],
        'favorite_count': record['favorite_count']
    }


def main():
    tweets = db.read_text(FIRST_FILE_PATH).map(json.loads)

    tweets_df = tweets.filter(lambda x: 'text' in x).map(
        text_flatten).to_dataframe().compute()
    build_search_index(tweets_df)


if __name__ == "__main__":
    main()
