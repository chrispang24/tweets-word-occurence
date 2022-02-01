## Word Occurence Processing

#### Goals

- Figure out how many times a particular word occurs in a large dataset (tweets)
- Segmentation of the word count by other attributes, such as retweet count, user id, geolocation, etc...

#### Data Input

Dataset: https://sample-twitter-dataset.s3.us-west-2.amazonaws.com/tweets.tar

To use with application code, download, unarchive and store at: /src/2017_01_01/

#### Install dependencies

```python
pipenv install
```

#### Build word count and word search index

build_word_count.py: saves word count into 'processed' directory
build_word_index.py: creates search index at 'search_index_dir' directory

```python
python app/build_word_count.py
python app/build_word_index.py
```

#### Search word count and word index

```python
python app/search_word_count.py
python app/search_word_index.py
```

#### Notes

Search word index solution only loads and searches on first JSON file.

'dataset_review.ipynb' used for early exploratory analysis on dataset.

'build_word_count.py' needs to be extended further in future to save Tweet metadata to data store.

'search_word_index.py' needs to be extended further in future to use returned Tweet Ids from Search Index search to query into Tweet metadata store and provide aggregations on word count based on segementation attributes like User ID etc.
