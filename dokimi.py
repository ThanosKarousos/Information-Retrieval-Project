#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from pandas.core.frame import DataFrame
from pandasticsearch import Select
import pandas as pd


test_df = pd.DataFrame(data=[[1,2,3]]*5, index=range(3, 8), columns = ['a','b','c'])
testDict = {'key1':['a', 'c'], 'key2': ['a', 'b'], 'key3': ['b', 'c']}
averageTest = pd.DataFrame()

for key in testDict:
    temp_df = test_df[testDict[key]]
    averageTest[key] = temp_df.mean(axis=1)
    print(temp_df)
    print('-------------------------------------------')

print(test_df)

print(averageTest)
