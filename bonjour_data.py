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

        # self.es.indices.delete(index='bonjour')
        # self.es.indices.delete(index='bonjour_tags')
        # self.es.indices.delete(index='bonjour_recommend')
        # self.es.indices.delete(index='bonjour_citys')
        if self.es.indices.exists(index='bonjour') is not True:
            tags = {}
            recommend = {}
            print("开始创建！")
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
                "mappings": {
                    "spot": {
                        "_all": {
                            "type": "text",
                            "fields": {
                                "cn": {
                                    "type": "text",
                                    "analyzer": "ik_smart",
                                    "search_analyzer": "ik_smart"
                                },
                                "en": {
                                    "type": "text",
                                    "analyzer": "english",
                                    "search_analyzer": "english",
                                }
                            }
                        },
                        "properties": {
                            "location": {
                                "type": "geo_point"
                            }
                        }
                    }
                }
            }
            other_index = {
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
                }
            }
            ret_bonjour = self.es.indices.create(index='bonjour', body=bonjour_index, ignore=400)
            print(ret_bonjour)
            ret_bonjour_tags = self.es.indices.create(index='bonjour_tags', body=other_index, ignore=400)
            print(ret_bonjour_tags)
            ret_bonjour_recommend = self.es.indices.create(index='bonjour_recommend', body=other_index, ignore=400)
            print(ret_bonjour_recommend)
            ret_bonjour_citys = self.es.indices.create(index='bonjour_citys', body=other_index, ignore=400)
            print(ret_bonjour_citys)
            # rootdir = 'pics'
            # pics = {}
            # for parent, dirnames, filenames in os.walk(rootdir):
            #     spotsname = parent.split('/')[-1]
            #     if (spotsname == 'pics'):
            #         continue
            #     pic = []
            #     for filename in filenames:
            #         if filename[-4:]=='html':continue
            #         filepath = 'http://182.254.227.188/'+os.path.join(parent, filename)
            #         name=filename.split('.')[0]
            #         pic.append({'tag': '', 'name': name, 'filepath': filepath})
            #     pics[spotsname] = pic
            num = 0
            with open('spots.json')as f:
                for line in f:
                    spot = json.loads(line.strip())
                    print('a', num)
                    num += 1
                    self.es.index(index='bonjour', doc_type='spot', refresh=False, body=spot, id=spot['sid'])
                    if spot['city'] != '':
                        if spot['city'] not in city_province:
                            city_province[spot['city']] = spot['province']
                    # if spot['sid'] in pics:
                    #     spot['pics'] = pics[spot['sid']]
                    if spot['name'] not in recommend:
                        recommend[spot['name']] = 0
                    for tag in spot['tags']:
                        if tag not in recommend:
                            recommend[tag] = 0
                        if tag not in tags:
                            tags[tag] = {}
                        if str(spot['city']) == '': continue
                        if spot['city'] not in tags[tag]:
                            tags[tag][spot['city']] = []
                        tags[tag][spot['city']].append(spot['sid'])
            num = len(recommend)
            for name in recommend:
                print('b', num)
                num -= 1
                self.es.index(index='bonjour_recommend', doc_type='recommend', refresh=False, body={'name': name})
            num = len(tags)
            for tag in tags:
                print('c', num)
                num -= 1
                for city in tags[tag]:
                    self.es.index(index='bonjour_tags', doc_type='tag', refresh=False,
                                  body={'tag': tag, 'city': city, 'sid_list': tags[tag][city]})
            num = len(city_province)
            for city in city_province:
                print('d', num)
                num -= 1
                self.es.index(index='bonjour_citys', doc_type='city', refresh=False,
                              body={'city': city, 'province': city_province[city]})
            print('创建结束！')

    def add(self):
        return None

    def delete(self):
        return None

    def updata(self):
        spot = self.es.get(index='bonjour', doc_type='spot', id='7694673')['_source']
        spot['images'] = ["http://182.254.227.188/pics/bonjour.jpg"]
        self.es.index(index='bonjour', doc_type='spot', refresh=False, body=spot, id=spot['sid'])
        spot = self.es.get(index='bonjour', doc_type='spot', id='15261326')['_source']
        spot['images'] = ["http://182.254.227.188/pics/bonjour.jpg"]
        self.es.index(index='bonjour', doc_type='spot', refresh=False, body=spot, id=spot['sid'])
        spot = self.es.get(index='bonjour', doc_type='spot', id='6631374')['_source']
        spot['images'] = ["http://182.254.227.188/pics/bonjour.jpg"]
        self.es.index(index='bonjour', doc_type='spot', refresh=False, body=spot, id=spot['sid'])
        spot = self.es.get(index='bonjour', doc_type='spot', id='6631375')['_source']
        spot['images'] = ["http://182.254.227.188/pics/bonjour.jpg"]
        self.es.index(index='bonjour', doc_type='spot', refresh=False, body=spot, id=spot['sid'])


es = Bonjour_ES()
es.updata()
print("你好啊！！！")
