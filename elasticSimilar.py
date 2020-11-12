#!/usr/bin/env python3

from elasticsearch import Elasticsearch

es = Elasticsearch(
  [{'host': 'localhost', 'port': 9200}])

searchTerm = input("What do you want to search for? ")

query_body = {
  "query": {
      "match": {
          "title": searchTerm #isws 8elei more_like
      }
  }
}

#to size einai epeidi by default epistrefei mono 10 items sto search
result = es.search(index="movie_index1", body=query_body, size=999)

numberOfHits = result['hits']['total']['value']

if(numberOfHits == 0):
  print("There are no movie titles of this kind")

elif(numberOfHits == 1):
  print("Got %d Hit:" % numberOfHits)

else:
  print("Got %d Hits:" % numberOfHits)
  #or: print("Got " + str(numberOfHits) + " Hits:")


for hit in result['hits']['hits']:
    print(hit["_source"]["title"] + " " + hit["_source"]["genres"])


##################################

#pros to paron emfanizei apotelesma mono otan yparxei 
#exact match tou query me ton titlo
#px: An query="gin", den 8a emfanisei to "Origins" sta apotelesmata
#paroti to gin emperiexetai sto Origins