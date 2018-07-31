from elasticsearch import Elasticsearch
import csv
import os
import re
import time
import json
import random

datapath = '/home/ubuntu/data/bonjour/data'
os.chdir(datapath)
attributes_list = ['id','name', 'intro', 'tags', 'plays','supplement','lng','lat','formatted_address','country','province' ,'city','district']
tags={}
recommend = {}
city_province = {}


class Bonjour_ES:
    es = Elasticsearch([{"host": "127.0.0.1", "port": 9200, "timeout": 3600}])

    def __init__(self):
        # self.es.indices.delete(index='bonjour')
        # self.es.indices.delete(index='bonjour_id')
        # self.es.indices.delete(index='bonjour_tags')
        # self.es.indices.delete(index='bonjour_recommend')
        # self.es.indices.delete(index='bonjour_city')
        if self.es.indices.exists(index='bonjour') is not True:
            print("bonjour 开始创建！")
            print("bonjour_id 开始创建！")
            print("bonjour_tags 开始创建！")
            print("bonjour_recommend 开始创建！")
            print("bonjour_city 开始创建！")
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
            ret_bonjour_city = self.es.indices.create(index='bonjour_city', body=bonjour_index, ignore=400)
            print(ret_bonjour_city)
            rootdir = 'pics'
            pics = {}
            for parent, dirnames, filenames in os.walk(rootdir):
                attractionsname = parent.split('/')[-1]
                if (attractionsname == 'pics'):
                    continue
                pic = []
                for filename in filenames:
                    if filename[-4:]=='html':continue
                    filepath = os.path.join(parent, filename)
                    name=filename.split('.')[0]
                    pic.append({'tag': '', 'name': name, 'filepath': filepath})
                pics[attractionsname] = pic
            with open('attraction.csv', 'r', encoding='utf8') as f:
                for line in csv.reader(f):
                    print(line[1])
                    attraction = {}
                    attraction['id'] = line[0]
                    attraction['name'] = line[1]
                    attraction['intro'] = line[2]
                    attraction['lng'] = line[3]
                    attraction['lat'] = line[4]
                    location=json.loads(re.sub("'",'"',line[5]))
                    attraction['formatted_address']=location['regeocode']['formatted_address']
                    attraction['country'] = location['regeocode']['addressComponent']['country']
                    attraction['province'] = location['regeocode']['addressComponent']['province']
                    if location['regeocode']['addressComponent']['city']!=[]:
                        attraction['city'] = location['regeocode']['addressComponent']['city']
                        if location['regeocode']['addressComponent']['city'] not in city_province:
                            city_province[location['regeocode']['addressComponent']['city']] = \
                            location['regeocode']['addressComponent']['province']
                    else:attraction['city']=''
                    attraction['district'] = location['regeocode']['addressComponent']['district']
                    attraction['tags'] = json.loads(re.sub("'",'"',line[6]))
                    if line[1] in pics:
                        attraction['pic'] = pics[line[1]]
                    self.es.index(index='bonjour', doc_type='attractions', refresh=True, body=attraction, id=line[0])
                    name_id = {}
                    name_id['name'] = line[1]
                    name_id['id'] = line[0]
                    self.es.index(index='bonjour_id', doc_type='id', refresh=True, body=name_id)
                    if line[1] not in recommend:
                        recommend[line[1]]=0
                    for tag in attraction['tags']:
                        if tag not in recommend:
                            recommend[tag]=0
                        if tag not in tags:
                            tags[tag]=[]
                        tags[tag].append(line[0])
            for name in recommend:
                self.es.index(index='bonjour_recommend', doc_type='recommend', refresh=True, body={'name': name})
            for tag in tags:
                self.es.index(index='bonjour_tags', doc_type='tags', refresh=True,
                              body={'tag': tag, 'attractions_id_list': tags[tag]})
            for city in city_province:
                self.es.index(index='bonjour_city',doc_type='city',refresh=True,body={'city':city,'province':city_province[city]})
            print('bonjour 创建结束！')
            print('bonjour_id 创建结束！')
            print('bonjour_tags 创建结束！')
            print("bonjour_recommend 创建结束！")
            print("bonjour_city 创建结束！")
        search = {'query': {'match_all': {}}}
        allDoc = self.es.search(index='bonjour_city', doc_type='city',size=10000, body=search)
        for doc in allDoc['hits']['hits']:
            city_province[doc['_source']['city']]=doc['_source']['province']
    def add(self):
        return None

    def delete(self):
        return None

    def updata(self):
        return None

    def get_recommend(self, params):
        returnlist = []
        try:
            scroll_id = params['scroll_id']
            allDoc = self.es.scroll(scroll_id=params['scroll_id'],scroll="5m")
        except:
            search = {'query': {'match': {'name': params['query']}}}
            allDoc = self.es.search(index='bonjour_recommend', doc_type='recommend', scroll="5m", size=10,
                                    body=search)
            scroll_id = allDoc['_scroll_id']
        for doc in allDoc['hits']['hits']:
            returnlist.append(doc['_source']['name'])
        return {'data':returnlist,'scroll_id':scroll_id}

    def get_search(self, params):
        returnlist = []
        try:
            scroll_id = params['scroll_id']
            allDoc = self.es.scroll(scroll_id=scroll_id,scroll="5m")
        except:
            if params['city'] in city_province:
                search={"query": {"bool": {"should":[{"match": {"name": params['query']}},{"match": {"province": city_province[params['city']]}}]}}}
            else:
                search = {'query': {'match': {'name': params['query']}}}
            allDoc = self.es.search(index='bonjour', doc_type='attractions', scroll="5m",body=search)
            scroll_id=allDoc['_scroll_id']
        for doc in allDoc['hits']['hits']:
            returnlist.append(doc['_source'])
        return {'data': returnlist,'scroll_id':scroll_id}

    def get_tags(self,params):
        returnlist = []
        try:
            scroll_id = params['scroll_id']
            allDoc = self.es.scroll(scroll_id=scroll_id,scroll="5m")
        except:
            search = {'query': {'match_all': {}}}
            allDoc = self.es.search(index='bonjour_tags', doc_type='tags', scroll="5m", size=10,
                                    body=search)
            scroll_id=allDoc['_scroll_id']
        for doc in allDoc['hits']['hits']:
            returnlist.append(doc['_source']['tag'])
        return {'data': returnlist, 'scroll_id': scroll_id}

    def get_taglist(self,params):
        returnlist = []
        search = {'query': {'match': {'tag':params['tag']}}}
        allDoc = self.es.search(index='bonjour_tags', doc_type='tags',size=10,
                                body=search)
        for doc in allDoc['hits']['hits']:
            taglist=doc['_source']['attractions_id_list']
            random.shuffle(taglist)
            times=0
            for id in taglist:
                if times==10:break
                times+=1
                returnlist.append(self.get_attractions({'id': id}))
            break
        return {'data': returnlist}

    def get_attractions(self, params):
        try:
            ret = self.es.get(index='bonjour', doc_type='attractions', id=params['id'])['_source']
            return ret
        except:
            return None

    def get_nearby(self,params):
        ######可以减少返回的字段
        returnlist=[]
        now_attraction=self.get_attractions(params)
        search = {'query': {'bool': {'must':[{'match':{'district':now_attraction['district']}},{'match':{'city':now_attraction['city']}}]}}}
        allDoc = self.es.search(index='bonjour', doc_type='attractions', size=11,
                                body=search)
        if len(allDoc['hits']['hits']) < 2:
            search = {'query': {'match': {'city': now_attraction['city']}}}
            allDoc = self.es.search(index='bonjour', doc_type='attractions', size=11,
                                body=search)
        if len(allDoc['hits']['hits'])<2:
            search = {'query': {'match': {'province': now_attractions['province']}}}
            allDoc = self.es.search(index='bonjour', doc_type='attractions', size=11,
                                    body=search)
        for doc in allDoc['hits']['hits']:
            if doc['_source']['id']!=params['id']:
                returnlist.append(doc['_source'])
        return {'data':returnlist}

    def get_similar(self,params):
        ######可以减少返回的字段
        returnlist=[]
        now_attraction=self.get_attractions(params)
        id_times={}
        for tag in now_attraction['tags']:
            search = {'query': {'match': {'tag': tag}}}
            allDoc = self.es.search(index='bonjour_tags', doc_type='tags', size=1,
                                body=search)
            for doc in allDoc['hits']['hits']:
                for id in doc['_source']['attractions_id_list']:
                    if id not in id_times:
                        id_times[id]=0
                    id_times[id]+=1
        id_times=sorted(id_times.items(),key=lambda x:x[1],reverse=True)
        for i in range(min(11,len(id_times))):
            if id_times[i][0]==params['id']:continue
            returnlist.append(self.get_attractions({'id':id_times[i][0]}))
        return {'data':returnlist}

es = Bonjour_ES()
print("你好啊！！！")
