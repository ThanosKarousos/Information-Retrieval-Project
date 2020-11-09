#!/usr/bin/env python3

from elasticsearch import Elasticsearch

es = Elasticsearch(
  [{'host': 'localhost', 'port': 9200}])
print(es)

searchTerm = input("What do you want to search for? ")

query_body = {
  "query": {
      "match": {
          "title": searchTerm
      }
  }
}

es.search(index="movie_index1", body=query_body)

#to size einai epeidi by default epistrefei mono 10 items sto search
result = es.search(index="movie_index1", body=query_body, size=999)
print ("total hits:", len(result["hits"]["hits"]))