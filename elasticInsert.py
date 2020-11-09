#!/usr/bin/env python3

from elasticsearch import helpers, Elasticsearch
import csv

es = Elasticsearch(
  [{'host': 'localhost', 'port': 9200}])
print(es)

#profanws edw vazoume to antistoixo path gia to movies.csv
csvFilePath = '/home/thanos/Desktop/CEID/semester9/infoRetrieval/project/movies.csv'

with open(csvFilePath) as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='movie_index1')