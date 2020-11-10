#!/usr/bin/env python3

from elasticsearch import helpers, Elasticsearch
import csv

es = Elasticsearch(
  [{'host': 'localhost', 'port': 9200}])
print(es)

indexName = "movie_index1"

#profanws edw vazoume to antistoixo path gia to movies.csv
csvFilePath = '/home/thanos/Desktop/CEID/semester9/infoRetrieval/project/movies.csv'

#vazoume similarity = BM25, opote by default psaxnei me to metric pou 8eloume
request_body = {
	    "settings" : {
	        "number_of_shards": 1,
	        "number_of_replicas": 1,
          "similarity": {
             "bm25-inverse-zero": {
                "type": "BM25",
                "b": 0
             }
	    }
	}
}

print("Creating " + indexName)
es.indices.create(index = indexName, body = request_body)

with open(csvFilePath) as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index= indexName)