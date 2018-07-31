from elasticsearch import Elasticsearch
import csv
import os
import time
import json

datapath = '/home/ubuntu/data/bonjour/data'
os.chdir(datapath)
attributes_list = ['id','name', 'intro', 'lng','lat','tags', 'plays', 'supplement']

attractions_id = {}
tags = {}


class Bonjour_ES:
    es = Elasticsearch([{"host": "127.0.0.1", "port": 9200, "timeout": 3600}])

    def __init__(self):
        if self.es.indices.exists(index='bonjour') is not True:
            print("bonjour 开始创建！")
            print("bonjour_id 开始创建！")
            print("bonjour_tags 开始创建！")
            print("bonjour_recommend 开始创建！")
            bonjour_index = {
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "bonjour_analyzer": {
                                "type": "ik_smart",
                                "stopwords_path": "stopwords.txt",
                                "max_token_length": 8,
                                "search_analyzer": "ik_smart"
                            }
                        }
                    }
                },
                # "mappings": {
                #     "attractions": {
                #         "_all": {
                #             "type": "text",
                #             "fields": {
                #                 "cn": {
                #                     "type": "text",
                #                     "analyzer": "ik_smart",
                #                     "search_analyzer": "ik_smart"
                #                 },
                #                 "en": {
                #                     "type": "text",
                #                     "analyzer": "english",
                #                     "search_analyzer": "english",
                #                 }
                #             }
                #         }
                #     }
                # }
            }
            ret_bonjour = self.es.indices.create(index='bonjour', body=bonjour_index, ignore=400)
            print(ret_bonjour)
            ret_bonjour_id = self.es.indices.create(index='bonjour_id', body=bonjour_index, ignore=400)
            print(ret_bonjour_id)
            ret_bonjour_tags = self.es.indices.create(index='bonjour_tags', body=bonjour_index, ignore=400)
            print(ret_bonjour_tags)
            ret_bonjour_recommend = self.es.indices.create(index='bonjour_recommend', body=bonjour_index, ignore=400)
            print(ret_bonjour_recommend)
            rootdir = 'pics'
            pics = {}
            for parent, dirnames, filenames in os.walk(rootdir):
                attractionsname = parent.split('/')[-1]
                pic = []
                for filename in filenames:
                    filepath = os.path.join(parent, filename)
                    filesplit = filename.split('-')
                    if len(filesplit) == 1:
                        filesplit = filename.split('.')
                    tag = filesplit[0]
                    if len(filesplit) <= 2:
                        name = tag
                    else:
                        name = filesplit[1]
                    pic.append({'tag': tag, 'name': name, 'filepath': filepath})
                if (attractionsname == 'pics'):
                    continue
                pics[attractionsname] = pic
            with open('bonjour.csv', 'r', encoding='utf8') as f:
                linenum = -1
                for line in csv.reader(f):
                    linenum += 1
                    if linenum == 0:
                        continue
                    attractions = {}
                    attractions['id'] = linenum
                    for u, v in enumerate(line):
                        if attributes_list[u] == "tags":
                            values = list(v.split('、'))
                            for value in values:
                                if value not in tags:
                                    tags[value] = []
                                tags[value].append(linenum)
                        if attributes_list[u] == "tags" or attributes_list[u] == "plays":
                            value = list(v.split('、'))
                            attractions[attributes_list[u]] = value
                        else:
                            attractions[attributes_list[u]] = v
                    attractions['pic'] = pics[line[0]]
                    self.es.index(index='bonjour', doc_type='attractions', refresh=True, body=attractions, id=linenum)
                    name_id = {}
                    name_id['name'] = line[0]
                    name_id['id'] = linenum
                    self.es.index(index='bonjour_id', doc_type='attractions', refresh=True, body=name_id)
                    self.es.index(index='bonjour_recommend', doc_type='recommend', refresh=True, body={'name': line[0]})
            for tag in tags:
                self.es.index(index='bonjour_tags', doc_type='tags', refresh=True,
                              body={'tag': tag, 'attractions_id_list': tags[tag]})
                self.es.index(index='bonjour_recommend', doc_type='recommend', refresh=True, body={'name': tag})
            print('bonjour 创建结束！')
            print('bonjour_id 创建结束！')
            print('bonjour_tags 创建结束！')
            print("bonjour_recommend 创建结束！")
        else:
            query = {'query': {'match_all': {}}}
            allDoc = self.es.search(index='bonjour_id', doc_type='attractions', body=query)
            for doc in allDoc['hits']['hits']:
                attractions_id[doc['_source']['name']] = doc['_source']['id']
            allDoc = self.es.search(index='bonjour_tags', doc_type='tags', body=query)
            for doc in allDoc['hits']['hits']:
                tags[doc['_source']['tag']] = doc['_source']['attractions_id_list']

    def add(self):
        return None

    def delete(self):
        return None

    def updata(self):
        return None

    def search_recommend(self, query):
        returnlist = []
        search = {'query': {'match': {'name': query}}}
        allDoc = self.es.search(index='bonjour_recommend', doc_type='recommend', body=search)
        for doc in allDoc['hits']['hits']:
            returnlist.append(doc['_source']['name'])
        return returnlist

    def search(self, search_data):
        returnlist = []
        if search_data['query'] in attractions_id:
            returnlist.append(
                self.es.get(index='bonjour', doc_type='attractions', id=attractions_id[search_data['query']])[
                    '_source'])
            return {'flag': 0, 'data': returnlist}
        for name in attractions_id:
            returnlist.append(self.es.get(index='bonjour', doc_type='attractions', id=attractions_id[name])['_source'])
        return {'flag': 1, 'data': returnlist}

    def get_tags(self):
        returnlist = []
        for tag in tags:
            returnlist.append({'tag':tag,'attractions_id_list':tags[tag]})
        return returnlist

    def get_attractions(self, id):
        try:
            ret = self.es.get(index='bonjour', doc_type='attractions', id=id)['_source']
            return ret
        except:
            return None

    def get_weather(self, reqdata):
        return None

    def get_position(self, reqdata):
        return None


es = Bonjour_ES()
es = Elasticsearch([{"host": "127.0.0.1", "port": 9200, "timeout": 3600}])
print("你好啊！！！")
es.indices.delete(index='bonjour')
es.indices.delete(index='bonjour_id')
es.indices.delete(index='bonjour_tags')
es.indices.delete(index='bonjour_recommend')
