#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from pandas.core.frame import DataFrame
from pandasticsearch import Select
import pandas as pd
from sklearn.cluster import KMeans

#################CLUSTERING PROCESS BEGIN########################
query_body = {
        "query": {
            "match_all": {}
            }
}

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
result = es.search(index="movie_index1",
            body=query_body, size=9999)

prev_pandas_df = Select.from_dict(result).to_pandas()

pandas_df = prev_pandas_df.drop(columns=['_index', '_type', '_id', '_score'])
print("Please wait for the clustering process to end. This might take a few minutes.")
new_df = pd.DataFrame()
averageByGenre = pd.DataFrame()
genresDict = {}
for index, row in pandas_df.iterrows():
    temp_df = pd.DataFrame(row['ratingArr'])
    new_df = new_df.append(temp_df, ignore_index=True)
    genres = row['genres']
    genres = genres.split('|')
    if not temp_df.empty:
        for genre in genres:
            if genre not in genresDict:
                genresDict[genre] = [int(row['movieId'])]
            else:
                genresDict[genre].append(int(row['movieId']))

new_df['userId'] = new_df['userId'].astype(int)
new_df['movieId'] = new_df['movieId'].astype(int)
new_df['rating'] = new_df['rating'].astype(float)
new_df = new_df.pivot(index='userId', columns='movieId', values='rating')

print("Almost ready...")

for key in genresDict:
    temp_df = new_df[genresDict[key]]
    averageByGenre[key] = temp_df.mean(axis=1, skipna=True)

averageByGenre = averageByGenre.fillna(0)

clusteringRes = KMeans(n_clusters = 5, algorithm='full', random_state=2).fit_predict(averageByGenre)

new_df['clustering'] = pd.Series(clusteringRes, index=new_df.index)

finalUserRating_df = pd.DataFrame()
for cluster in new_df['clustering'].unique():
    temp_df = new_df.loc[new_df['clustering'] == cluster]
    temp_df = temp_df.fillna(temp_df.mean(skipna=True))
    finalUserRating_df = finalUserRating_df.append(temp_df)

finalUserRating_df = finalUserRating_df.drop('clustering', axis=1).transpose()
print("Clustering process finished!")
#finalUserRating_df is the dataframe with the NaN are replaced by the average rating of
#the cluster each user belongs to
###################CLUSTERING PROCESS END################################



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

searchTerm = input("What do you want to search for? ")
userIDsearch = input("What is your userId? ")

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
movieIds = pandas_df['movieId'].astype(int)
scores = pandas_df['_score'].astype(float)

resultDataframe = pd.concat([movieIds, titles, genres, scores], axis=1, sort=False, join='outer')
finalResultDataframe = pd.merge(resultDataframe, finalUserRating_df[int(userIDsearch)], on='movieId')
finalResultDataframe['finalScore'] = finalResultDataframe['_score'] + finalResultDataframe[int(userIDsearch)]
finalResultDataframe = finalResultDataframe.sort_values(by='finalScore', ascending=False).reset_index().drop([int(userIDsearch), 'index'], axis=1)

print(finalResultDataframe)
