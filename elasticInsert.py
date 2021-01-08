#!/usr/bin/env python3

from elasticsearch import helpers, Elasticsearch
import csv
import operator

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
            "genres":  {"type": "text"}
             #No array handler
        }
    }
}

print("Creating " + indexName)
es.indices.create(index=indexName, body=request_body)

with open(csvFilePath, encoding="utf-8") as movies, open('ratings.csv', encoding="utf-8") as ratings:
    movieReader = csv.DictReader(movies)
    ratingReader = csv.DictReader(ratings)
    sortedRating = sorted(ratingReader, key=lambda row: int(row['movieId'])) #sorting ratings by movieId for faster insert
    z = 0
    rows = []
    for row in movieReader:
        row['ratingArr'] = []
        for i in range(z, len(sortedRating)):
            if sortedRating[i]['movieId'] == row['movieId']:
                gen = {"rating": sortedRating[i]['rating'], "userId": sortedRating[i]['userId']}
                row['ratingArr'].append(gen)
            else:
                z=i #so for loop can continue from where it stopped (break)
                break
        rows.append(row)
    helpers.bulk(es, rows, index=indexName)
