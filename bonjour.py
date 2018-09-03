from flask import Flask, request
from bonjour_es import Bonjour_ES
from flask_cors import *
import json
import os

app = Flask(__name__)
CORS(app, supports_credentials=True)
es = Bonjour_ES()
datapath = '/home/ubuntu/data/bonjour/data'
os.chdir(datapath)


@app.route("/hello", methods=['GET'])
def hello():
    return "欢迎来到笨猪旅行~~~"


query = '乐山大佛'
tags = ['日出', '雪山']
days = 1
adcode = '110000'


@app.route("/v1/api/spots_by_loc_distance", methods=['GET'])
def get_spots_by_loc_distance():
    ret = es.get_spots_by_loc_distance(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/spots_by_loc_distance_range", methods=['GET'])
def get_spots_by_loc_distance_range():
    ret = es.get_spots_by_loc_distance_range(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/spots_by_loc_distance_tags", methods=['GET'])
def get_spots_by_loc_distance_tags():
    ret = es.get_spots_by_loc_distance_tags(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/spots_by_loc_distance_tags_range", methods=['GET'])
def get_spots_by_loc_distance_tags_range():
    ret = es.get_spots_by_loc_distance_tags_range(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/tags_by_loc_distance", methods=['GET'])
def get_tags_by_loc_distance():
    ret = es.get_tags_by_loc_distance(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/tags_by_loc_distance_range", methods=['GET'])
def get_tags_by_loc_distance_range():
    ret = es.get_tags_by_loc_distance_range(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/tags_by_city", methods=['GET'])
def get_tags_by_city():
    ret = es.get_tags_by_city(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/chatmessage", methods=['GET'])
def get_chatmessage():
    user_flag = request.args['user_flag']
    message = json.loads(request.args['message'])
    size = message['size']
    from_page = int(message['from_page']) * int(size)
    if user_flag == '0':
        query = message['query']
        if '天气' in query:
            return json.dumps({"flag": 5, "message": {"local": "深圳", "start_time": "2018-08-08 13:24:16",
                                                      "delta_time": "2 days 0:00:00"}})
        if '调出智能助手' in message['query'] or '智能助手' in message['query'] or '怎么使用' in message['query']:
            return json.dumps({'flag': 1, 'message': {'text': '不知道去哪儿浪，交给我~'}})
        jingdian = es.get_spots_by_query_adcode_tags_days(
            {'from_page': from_page, 'size': size, 'adcode': adcode, 'query': query, 'tags': tags, 'days': days})
        if len(jingdian['data']) > 1:
            return json.dumps({'flag': 3, 'message': jingdian})
        if len(jingdian['data']) == 1:
            return json.dumps({'flag': 4, 'message': jingdian})
        return json.dumps({'flag': 0, 'message': {'text': '欢迎来到笨猪旅行~~~'}})
    elif user_flag == '1':
        adcode = message["adcode"]
        days = message["days"]
        returndata = es.get_tags_by_adcode_days({'from_page': from_page, 'size': size, 'adcode': adcode, 'days': days})
        returndata.update({'text': '您可能感兴趣的标签：'})
        return json.dumps({'flag': 2, 'message': returndata})
    elif user_flag == '2':
        tags = message['select_tags']
        if len(message['input_tag']) > 0:
            tags.append(message['input_tag'])
        return json.dumps({'flag': 3, 'message': es.get_spots_by_adcode_tags_days(
            {'from_page': from_page, 'size': size, 'adcode': adcode, 'tags': tags, 'days': days})})
    return None


@app.route("/v1/api/areaInfo", methods=['GET'])
def get_areaInfo():
    ret = json.load(open('areaInfo.json'))
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/recommend", methods=['GET'])
def get_recommend():
    ret = es.get_recommend(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/spots_by_query_adcode_tags_days", methods=['GET'])
def get_spots_by_query_adcode_tags_days():
    size = int(request.args['size'])
    from_page = int(request.args['from_page']) * size
    query = request.args['data']['query']
    adcode = request.args['data']['adcode']
    tags = request.args['data']['tags']
    days = request.args['data']['days']
    ret = es.get_spots_by_query_adcode_tags_days(
        {'from_page': from_page, 'size': size, 'query': query, 'adcode': adcode, 'tags': tags, 'days': days})
    ret = json.dumps({'falg': 3, 'message': ret})
    return ret


@app.route("/v1/api/spots_by_adcode_tags_days", methods=['GET'])
def get_spots_by_adcode_tags_days():
    size = int(request.args['size'])
    from_page = int(request.args['from_page']) * size
    adcode = request.args['data']['adcode']
    tags = request.args['data']['tags']
    days = request.args['data']['days']
    ret = es.get_spots_by_adcode_tags_days(
        {'from_page': from_page, 'size': size, 'adcode': adcode, 'tags': tags, 'days': days})
    ret = json.dumps({'falg': 3, 'message': ret})
    return ret


@app.route("/v1/api/tags_by_adcode_days", methods=['GET'])
def get_tags_by_adcode_days():
    size = int(request.args['size'])
    from_page = int(request.args['from_page']) * size
    adcode = request.args['data']['adcode']
    days = request.args['data']['days']
    ret = es.get_tags_by_adcode_days({'from_page': from_page, 'size': size, 'adcode': adcode, 'days': days})
    ret = json.dumps({'flag': 2, 'message': ret})
    return ret


@app.route("/v1/api/spot", methods=['GET'])
def get_spot():
    ret = es.get_spot(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/nearby", methods=['GET'])
def get_nearby():
    sid = request.args['sid']
    size = int(request.args['size'])
    from_page = int(request.args['from_page']) * size
    ret = es.get_nearby({'sid': sid, 'from_page': from_page, 'size': size})
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/similar", methods=['GET'])
def get_similar():
    sid = request.args['sid']
    size = int(request.args['size'])
    from_page = int(request.args['from_page']) * size
    ret = es.get_similar({'sid': sid, 'from_page': from_page, 'size': size})
    ret = json.dumps(ret)
    return ret


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=45678, threaded=True)
