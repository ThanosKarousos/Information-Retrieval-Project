#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from pandasticsearch import Select
import pandas as pd


def makeQueryBody(field, value):
    query_body = {
        "query": {
            "match": {
                field: value
            }
        }
    }
    return query_body


es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200}])

searchTerm = input("What do you want to search for? ")
#userIDsearch = input("What is your userId? ")

# to size einai epeidi by default epistrefei mono 10 items sto search
result = es.search(index="movie_index1",
                   body=makeQueryBody("title", searchTerm), size=999)

numberOfHits = result['hits']['total']['value']

if(numberOfHits == 0):
    print("There are no movie titles of this kind")
    exit()


print("Got %d Hit(s):" % numberOfHits)
# or: print("Got " + str(numberOfHits) + " Hits:")

# creates a dataframe from result JSON
pandas_df = Select.from_dict(result).to_pandas()

titles = pandas_df['title']
genres = pandas_df['genres']
movieIds = pandas_df['movieId']
scores = pandas_df['_score']

ratings_list = []
for i in range(movieIds.size):

    queryBody1 = {
        "query": {
            "match": {
                "movieId": movieIds[i]
            }
        },
        "aggs": {
            "avg_movie_rating": {
                "avg":
                {
                    "field": "rating"
                }
            }
        }
    }

    ratingAVG = es.search(index="ratings_index1", body=queryBody1, size=999)

    movieRatingAVG = ratingAVG["aggregations"]["avg_movie_rating"]["value"]
    #r = {"movieID": movieIds[i], "avgRating": movieRatingAVG} ---mporei na xreiastei etsi
    #ratings_list.append(r)
    ratings_list.append(movieRatingAVG)

rating_df = pd.DataFrame(ratings_list, columns = ['AvgRating'])

movieDataframe = pd.concat(
    [movieIds, titles, genres, scores, rating_df], axis=1, sort=False, join='outer')
print(movieDataframe)
##################################

# pros to paron emfanizei apotelesma mono otan yparxei
# exact match tou query me ton titlo
# px: An query="gin", den 8a emfanisei to "Origins" sta apotelesmata
# paroti to gin emperiexetai sto Origins
