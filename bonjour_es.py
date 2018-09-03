from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv
import os
import re
import time
import json
import random

datapath = '/home/ubuntu/data/bonjour/data'
os.chdir(datapath)
city_province = {}
adcode_loc = {}


class Bonjour_ES:
    es = Elasticsearch([{"host": "127.0.0.1", "port": 9200, "timeout": 3600}])
    with open('area.csv') as f:
        ff = csv.reader(f)
        for line in ff:
            adcode_loc[line[0]] = line[2] + ',' + line[3]

    def __init__(self):
        search = {'query': {'match_all': {}}}
        allDoc = self.es.search(index='bonjour_citys', doc_type='city', size=10000, body=search)
        for doc in allDoc['hits']['hits']:
            city_province[doc['_source']['city']] = doc['_source']['province']

    def get_spots_by_loc_distance(self, params):
        returnlist = []
        geo_dict = {"geo_distance": {"unit": "km", "distance": params['distance'], "location": params['loc']}}
        search = {"query": {"bool": {"filter": geo_dict}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', body=search, from_=int(params['from_page']),
                                size=int(params['size']))
        for doc in allDoc['hits']['hits']:
            item = doc['_source']
            returnlist.append(
                {'sid': item['sid'], 'name': item['name'], 'tags': item['tags'], 'images': item['images']})
        return {'data': returnlist}

    def get_spots_by_loc_distance_range(self, params):
        returnlist = []
        geo_dict_max = {"geo_distance": {"unit": "km", "distance": params['dismax'], "location": params['loc']}}
        geo_dict_min = {"geo_distance": {"unit": "km", "distance": params['dismin'], "location": params['loc']}}
        search = {"query": {'bool': {"filter": {'bool': {'must': geo_dict_max, 'must_not': geo_dict_min}}}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', body=search, from_=int(params['from_page']),
                                size=int(params['size']))
        for doc in allDoc['hits']['hits']:
            item = doc['_source']
            returnlist.append(
                {'sid': item['sid'], 'name': item['name'], 'tags': item['tags'], 'images': item['images']})
        return {'data': returnlist}

    def get_spots_by_loc_distance_tags(self, params):
        returnlist = []
        tags_string = ''
        for tag in json.loads(params['tags']):
            tags_string += tag + ' '
        geo_dict = {"geo_distance": {"unit": "km", "distance": params['distance'], "location": params['loc']}}
        search = {"query": {"bool": {"should": {"match": {'tags_string': tags_string}, }, "filter": geo_dict}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', body=search, from_=int(params['from_page']),
                                size=int(params['size']))
        for doc in allDoc['hits']['hits']:
            print(doc['_score'])
            item = doc['_source']
            returnlist.append(
                {'sid': item['sid'], 'name': item['name'], 'tags': item['tags'], 'images': item['images']})
        return {'data': returnlist}

    def get_spots_by_loc_distance_tags_range(self, params):
        returnlist = []
        tags_string = ''
        for tag in json.loads(params['tags']):
            tags_string += tag + ' '
        geo_dict_max = {"geo_distance": {"unit": "km", "distance": params['dismax'], "location": params['loc']}}
        geo_dict_min = {"geo_distance": {"unit": "km", "distance": params['dismin'], "location": params['loc']}}
        search = {"query": {"bool": {"should": {"match": {'tags_string': tags_string}, },
                                     "filter": {'bool': {'must': geo_dict_max, 'must_not': geo_dict_min}}}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', body=search, from_=int(params['from_page']),
                                size=int(params['size']))
        for doc in allDoc['hits']['hits']:
            item = doc['_source']
            returnlist.append(
                {'sid': item['sid'], 'name': item['name'], 'tags': item['tags'], 'images': item['images']})
        return {'data': returnlist}

    def get_tags_by_loc_distance(self, params):
        print(params)
        returnlist = []
        geo_dict = {"geo_distance": {"unit": "km", "distance": params['distance'], "location": params['loc']}}
        search = {"query": {"bool": {"filter": geo_dict}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', body=search, from_=int(params['from_page']),
                                size=int(params['size']))
        tags = {}
        for doc in allDoc['hits']['hits']:
            for tag in doc['_source']['tags']:
                tags[tag] = 0
        t = 0
        for tag in tags:
            returnlist.append(tag)
            t += 1
            if t == int(params['size']): break
        return {'data': returnlist}

    def get_tags_by_loc_distance_range(self, params):
        returnlist = []
        geo_dict_max = {"geo_distance": {"unit": "km", "distance": params['dismax'], "location": params['loc']}}
        geo_dict_min = {"geo_distance": {"unit": "km", "distance": params['dismin'], "location": params['loc']}}
        search = {"query": {'bool': {"filter": {'bool': {'must': geo_dict_max, 'must_not': geo_dict_min}}}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', body=search, from_=int(params['from_page']),
                                size=int(params['size']))
        tags = {}
        for doc in allDoc['hits']['hits']:
            for tag in doc['_source']['tags']:
                tags[tag] = 0
        t = 0
        for tag in tags:
            returnlist.append(tag)
            t += 1
            if t == int(params['size']): break
        return {'data': returnlist}

    def get_tags_by_city(self, params):
        returnlist = []
        search = {'query': {'match': {'city': params['city']}}}
        allDoc = self.es.search(index='bonjour_tags', doc_type='tag', from_=int(params['from_page']),
                                size=int(params['size']),
                                body=search)
        for doc in allDoc['hits']['hits']:
            returnlist.append(doc['_source']['tag'])
        return {'data': returnlist}

    def get_recommend(self, params):
        returnlist = []
        search = {'query': {'match': {'name': params['query']}}}
        allDoc = self.es.search(index='bonjour_recommend', doc_type='recommend', from_=int(params['from_page']),
                                size=int(params['size']),
                                body=search)
        for doc in allDoc['hits']['hits']:
            returnlist.append(doc['_source']['name'])
        return {'data': returnlist}

    def get_tags_by_adcode_days(self, params):
        returnlist = []
        if days <= 1:
            dismax = 100
            dismin = -1
        elif days <= 2:
            dismax = 300
            dismin = 100
        elif days <= 3:
            dismax = 800
            dismin = 300
        elif days <= 4:
            dismax = 2000
            dismin = 800
        else:
            dismax = 5000
            dismin = 2000

        geo_dict_max = {"geo_distance": {"unit": "km", "distance": dismax, "location": adcode_loc[params['adcode']]}}
        geo_dict_min = {"geo_distance": {"unit": "km", "distance": dismin, "location": adcode_loc[params['adcode']]}}
        search = {"query": {'bool': {"filter": {'bool': {'must': geo_dict_max, 'must_not': geo_dict_min}}}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                size=int(params['size']),
                                body=search)
        tags = {}
        for doc in allDoc['hits']['hits']:
            for tag in doc['_source']['tags']:
                tags[tag] = 0
        t = 0
        for tag in tags:
            returnlist.append(tag)
            t += 1
            if t == int(params['size']): break
        return {'data': returnlist}

    def get_spots_by_adcode_tags_days(self, params):
        returnlist = []
        if days <= 1:
            dismax = 100
            dismin = -1
        elif days <= 2:
            dismax = 300
            dismin = 100
        elif days <= 3:
            dismax = 800
            dismin = 300
        elif days <= 4:
            dismax = 2000
            dismin = 800
        else:
            dismax = 5000
            dismin = 2000

        geo_dict_max = {"geo_distance": {"unit": "km", "distance": dismax, "location": adcode_loc[params['adcode']]}}
        geo_dict_min = {"geo_distance": {"unit": "km", "distance": dismin, "location": adcode_loc[params['adcode']]}}
        if 'tags_string' in params:
            tags_string = params['tags_string']
        else:
            tags_string = ''
            for tag in params['tags']:
                tags_string += tag + ' '
        search = {"query": {'bool': {'should': {"match": {"tags_string": tags_string}},
                                     "filter": {'bool': {'must': geo_dict_max, 'must_not': geo_dict_min}}}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                size=int(params['size']), body=search)
        for doc in allDoc['hits']['hits']:
            item = doc['_source']
            returnlist.append(
                {'sid': item['sid'], 'name': item['name'], 'tags': item['tags'], 'images': item['images']})
        return {'data': returnlist}

    def get_spots_by_query_adcode_tags_days(self, params):
        returnlist = []
        if days <= 1:
            dismax = 100
            dismin = -1
        elif days <= 2:
            dismax = 300
            dismin = 100
        elif days <= 3:
            dismax = 800
            dismin = 300
        elif days <= 4:
            dismax = 2000
            dismin = 800
        else:
            dismax = 5000
            dismin = 2000

        geo_dict_max = {"geo_distance": {"unit": "km", "distance": dismax, "location": adcode_loc[params['adcode']]}}
        geo_dict_min = {"geo_distance": {"unit": "km", "distance": dismin, "location": adcode_loc[params['adcode']]}}
        if 'tags_string' in params:
            tags_string = params['tags_string']
        else:
            tags_string = ''
            for tag in params['tags']:
                tags_string += tag + ' '
        search = {"query": {
            'bool': {'should': [{"match": {"tags_string": tags_string}}, {"match": {"name": params['query']}}],
                     "filter": {'bool': {'must': geo_dict_max, 'must_not': geo_dict_min}}}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                size=int(params['size']), body=search)
        for doc in allDoc['hits']['hits']:
            item = doc['_source']
            returnlist.append(
                {'sid': item['sid'], 'name': item['name'], 'tags': item['tags'], 'images': item['images']})
        return {'data': returnlist}

    def get_spots_by_tag(self, params):
        returnlist = []
        search = {'query': {'match': {'tags_string': params['tag']}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                size=int(params['size']), body=search)
        for doc in allDoc['hits']['hits']:
            item = doc['_source']
            returnlist.append(
                {'sid': item['sid'], 'name': item['name'], 'tags': item['tags'], 'images': item['images']})
        return {'data': returnlist}

    def get_spot(self, params):
        try:
            ret = self.es.get(index='bonjour', doc_type='spot', id=params['sid'])['_source']
            return ret
        except:
            return None

    def get_nearby(self, params):
        returnlist = []
        now_spot = self.get_spot(params)
        search = {'query': {
            'bool': {'must': [{'match': {'district': now_spot['district']}}, {'match': {'city': now_spot['city']}}]}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                size=int(params['from_page']) + 1,
                                body=search)
        if len(allDoc['hits']['hits']) < 2:
            search = {'query': {'match': {'city': now_spot['city']}}}
            allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                    size=int(params['from_page']) + 1,
                                    body=search)
        if len(allDoc['hits']['hits']) < 2:
            search = {'query': {'match': {'province': now_spot['province']}}}
            allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                    size=int(params['from_page']) + 1,
                                    body=search)
        for doc in allDoc['hits']['hits']:
            if doc['_source']['sid'] != params['sid']:
                item = doc['_source']
                returnlist.append(
                    {'sid': item['sid'], 'name': item['name'], 'tags': item['tags'], 'images': item['images']})
        return {'data': returnlist}

    def get_similar(self, params):
        returnlist = []
        now_spot = self.get_spot(params)
        search = {'query': {'bool': {
            'must': [{'match': {'tags_string': now_spot['tags_string']}}, {'match': {'city': now_spot['city']}}]}}}
        allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                size=int(params['size']) + 1,
                                body=search)
        if len(allDoc['hits']['hits']) < 2:
            search = {'query': {'bool': {
                'must': [{'match': {'tags_string': now_spot['tags_string']}},
                         {'match': {'province': now_spot['province']}}]}}}
            allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                    size=int(params['size']) + 1,
                                    body=search)
        if len(allDoc['hits']['hits']) < 2:
            search = {'query': {'match': {'tags_string': now_spot['tags_string']}}}
            allDoc = self.es.search(index='bonjour', doc_type='spot', from_=int(params['from_page']),
                                    size=int(params['size']) + 1,
                                    body=search)
        for doc in allDoc['hits']['hits']:
            if doc['_source']['sid'] != params['sid']:
                item = doc['_source']
                returnlist.append(
                    {'sid': item['sid'], 'name': item['name'], 'tags': item['tags'], 'images': item['images']})
        return {'data': returnlist}


es = Bonjour_ES()
print("你好啊！！！")
