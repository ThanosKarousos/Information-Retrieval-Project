#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from pandas.core.frame import DataFrame
from pandasticsearch import Select
import pandas as pd

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

#new_df = pd.DataFrame({'userIds': []})
new_df = pd.DataFrame()
averageByGenres = pd.DataFrame()
genresDict = {}
for index, row in pandas_df.iterrows():
    temp_df = pd.DataFrame(row['ratingArr'])
    new_df = new_df.append(temp_df, ignore_index=True)
    genres = row['genres']
    genres = genres.split('|')
    for genre in genres:
        if genre not in genresDict:
            genresDict[genre] = [row['movieId']]
        else:
            genresDict[genre].append(row['movieId'])

new_df['userId'] = new_df['userId'].astype(int)
new_df['movieId'] = new_df['movieId'].astype(int)
new_df['rating'] = new_df['rating'].astype(float)
#new_df = new_df.pivot(index='userId', columns='movieId', values='rating')

for key in genresDict:
    temp_df = new_df[genresDict[key]]
    averageByGenres[key] = temp_df.mean(axis=1)

print(averageByGenres)



#####################################################################
#df_ = pd.DataFrame(index='index', columns='columns')
#df_ = df_.fillna([]) # with 0s rather than NaNs
