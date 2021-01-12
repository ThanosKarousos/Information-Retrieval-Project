#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from pandas.core.frame import DataFrame
from pandasticsearch import Select
import pandas as pd


def makeQueryBody(movieTitle, givenId):
    query_body = {
        "query": {
    "bool": {
      "must": [
        {
          "match": {
            "title": movieTitle
          }
        }
      ],
      "should": [
        {
          "nested": {
            "path": "ratingArr",
            "query": {
              "function_score": {
                "query": {
                  "match_all": {}
                },
                "functions": [
                  {
                    "field_value_factor": {
                      "field": "ratingArr.rating"
                    }
                  },
                  {
                    "filter": {
                      "term": {
                        "ratingArr.userId": givenId
                      }
                    },
                    "weight": 5
                  }
                ]
              }
            }
          }
        }
      ]
    }
  },
  "size": 20,
  "aggs": {
    "into_ratings_array": {
      "nested": {
        "path": "ratingArr"
      },
      "aggs": {
        "each_movie": {
          "terms": {
            "field": "ratingArr.movieId",
            "size": 20,
            "order": {
              "avg_rating": "desc"
            }
          },
          "aggs": {
            "avg_rating": {
              "avg": {
                "script": "_score"
              }
            }
          }
        }
      }
    }
  }
    }
    return query_body


es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200}])

searchTerm = input("What do you want to search for? ")
userIDsearch = input("What is your userId? ")

# to size einai epeidi by default epistrefei mono 10 items sto search
result = es.search(index="movie_index1",
                   body=makeQueryBody(searchTerm, userIDsearch), size=999)

numberOfHits = result['hits']['total']['value']

if(numberOfHits == 0):
    print("There are no movie titles of this kind")
    exit()


print("Got %d Hit(s):" % numberOfHits)

pandas_df = Select.from_dict(result).to_pandas()

titles = pandas_df['title']
genres = pandas_df['genres']
movieIds = pandas_df['movieId']
scores = pandas_df['_score']

resultDataframe = pd.concat([movieIds, titles, genres, scores], axis=1, sort=False, join='outer')
print(resultDataframe)
