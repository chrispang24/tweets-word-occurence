import dask.dataframe as dd
from config_file import WORD_COUNT_PATH


def extract_frequencies(target_path):
    return dd.read_parquet(target_path)


def main():
    tweets_dd = extract_frequencies(WORD_COUNT_PATH)

    print("Enter a word to retrieve occurence frequency for:")
    search_term = input()

    result = tweets_dd[tweets_dd['word'] == search_term].compute()
    frequency = result.iloc[0]['frequency'] if len(result) > 0 else 0
    print(f"The word {search_term} occurs {frequency} times in the dataset.")


if __name__ == "__main__":
    main()
