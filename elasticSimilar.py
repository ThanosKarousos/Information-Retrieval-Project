#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from pandas.core.frame import DataFrame
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
userIDsearch = input("What is your userId? ")

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
    # r = {"movieID": movieIds[i], "avgRating": movieRatingAVG} ---mporei na xreiastei etsi
    # ratings_list.append(r)
    ratings_list.append(movieRatingAVG)

rating_df = pd.DataFrame(ratings_list, columns=['AvgRating'])

#movieDataframe = pd.concat(
#    [movieIds, titles, genres, scores, rating_df], axis=1, sort=False, join='outer')
#print(movieDataframe)
##################################

# pros to paron emfanizei apotelesma mono otan yparxei
# exact match tou query me ton titlo
# px: An query="gin", den 8a emfanisei to "Origins" sta apotelesmata
# paroti to gin emperiexetai sto Origins
userRatings = []


for i in range(movieIds.size):

    query2 = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "userId": userIDsearch
                        }
                    },
                    {
                        "match": {
                            "movieId": movieIds[i]
                        }
                    }
                ]
            }
        }
    }

    test = es.search(index="ratings_index1", body=query2, size=999)
    
    appendedRating = 0


    if(len(test["hits"]["hits"]) != 0):

        for num in (test['hits']['hits']):
            appendedRating = num['_source']['rating']


    userRatings.append(appendedRating)


userRatingsDF = DataFrame(userRatings, columns=['User_Rating'])

movieDataframe = pd.concat(
    [movieIds, titles, genres, scores, rating_df, userRatingsDF], axis=1, sort=False, join='outer')
print(movieDataframe)

# metric = 1.1*BM25 + 1.3*AvgRating + 1.5*userRating
# test: userID = 187, search = sex