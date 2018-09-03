from elasticsearch import Elasticsearch
import csv
import os
import re
import time
import json
import random
es = Elasticsearch([{"host": "127.0.0.1", "port": 9200, "timeout": 3600}])

search={
    "query": {
            "filter" : {
                "html" : {
                    "unit": "km",
                    "distance": 100,
                    "location" : "19.635768954804,109.54778324752"
                }
            }
        }
    }

#
# search={
#   "query":{
#       "bool":
#
#       "filter":{
#         "geo_distance_range":{
#         "gte": "100km",
#         "lt":  "500km",
#         "location":{
#           "lat":19.635768954804,
#           "lon": 109.54778324752
#         }
#       }
#     }
#   }
# }
