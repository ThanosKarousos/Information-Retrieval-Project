#!/usr/bin/env python3

from elasticsearch import helpers, Elasticsearch
import csv
from time import sleep

es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200}])

indexName = "movie_index1"

# profanws edw vazoume to antistoixo path gia to movies.csv
csvFilePath = 'movies.csv'

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
            "movieId": {"type": "integer"},
            "title":  {
                "type": "text",
                "analyzer": "english"
            },
            "genres":  {"type": "text"},
            "userId": {"type": "integer"},
            "rating":  {"type": "half_float"},
            "timestamp":  {"type": "integer"}
        }
    }
}


print("Creating " + indexName)
#es.indices.create(index=indexName, body=request_body)

with open(csvFilePath, encoding="utf-8") as movies:
    movieReader = csv.DictReader(movies)

    for row in movieReader:
        row['ratingArr'] = []
        with open('ratings.csv', encoding="utf-8") as ratings:
            ratingReader = csv.DictReader(ratings)
            for rating in ratingReader:
                if rating['movieId'] == row['movieId']:
                    gen = {"rating": rating['rating'], "userId": rating['userId']}
                    row['ratingArr'].append(gen)
        print(row)
    #helpers.bulk(es, reader, index=indexName)
