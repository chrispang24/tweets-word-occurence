import pandas as pd
from whoosh import index, qparser
from config_file import SEARCH_INDEX_DIR


def index_search(dirname, search_fields, search_query):
    ix = index.open_dir(dirname)
    schema = ix.schema

    og = qparser.OrGroup.factory(0.9)
    mp = qparser.MultifieldParser(search_fields, schema, group=og)

    q = mp.parse(search_query)

    with ix.searcher() as s:
        results = s.search(q, terms=True, limit=None)
        print(f"{len(results)} Search Results (displaying IDs): ")

        col_list = ["id", "text", "retweet_count", "favorite_count"]
        results_df = pd.DataFrame([[hit[col] for col in col_list]
                                   for hit in results], columns=col_list)
        print(list(results_df['id']))


def main():
    print("Enter a word to search tweets for:")
    search_term = input()

    index_search(SEARCH_INDEX_DIR, ['text'], search_term)


if __name__ == "__main__":
    main()
