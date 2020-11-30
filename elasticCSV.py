#!/usr/bin/env python3

# For inserting the ratings.csv file into a new index

from elasticsearch import helpers, Elasticsearch
import csv

es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200}])

indexName = "ratings_index1"

# profanws edw vazoume to antistoixo path gia to movies.csv
csvFilePath = 'ratings.csv'

# vazoume similarity = BM25, opote by default psaxnei me to metric pou 8eloume
request_body = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "similarity": {
            "default": {
                "type": "BM25",
                        "b": 0.5,
                        "k1": 0
            }
        }
    },
    "mappings": {
        "properties": {
            "userId": {"type": "integer"},
            "movieId": {"type": "integer"},
            "rating":  {"type": "half_float"},
            "timestamp":  {"type": "integer"}
        }
    }
}


print("Creating " + indexName)
es.indices.create(index=indexName, body=request_body)

with open(csvFilePath, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index=indexName)
