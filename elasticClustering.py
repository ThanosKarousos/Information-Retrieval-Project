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

new_df = pd.DataFrame({'userIds': []})
for index, row in pandas_df.iterrows():
    if row['genres'] not in new_df.columns:
        tempDf = pd.DataFrame()#to be continued (add column?)
    for ratingRow in row['ratingArr']:
        ratingRow['userId'] = int(ratingRow['userId'])
        if ratingRow['userId'] not in new_df['userIds'].values:
            new_df = new_df.append({'userIds': ratingRow['userId']}, ignore_index=True)

print(new_df)

#df_ = pd.DataFrame(index='index', columns='columns')
#df_ = df_.fillna([]) # with 0s rather than NaNs
