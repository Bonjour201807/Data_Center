from flask import Flask, request
from bonjour_es1 import Bonjour_ES
from flask_cors import *
import json

app = Flask(__name__)
CORS(app, supports_credentials=True)
es = Bonjour_ES()


@app.route("/hello", methods=['GET'])
def hello():
    return "欢迎来到笨猪旅行~~~"

@app.route("/v1/api/recommend", methods=['GET'])
def get_recommend():
    ret = es.get_recommend(request.args)
    ret=json.dumps(ret)
    return ret

@app.route("/v1/api/search", methods=['GET'])
def get_search():
    ret = es.get_search(request.args)
    ret = json.dumps(ret)
    return ret

@app.route("/v1/api/tags", methods=['GET'])
def get_tags():
    ret = es.get_tags(request.args)
    ret = json.dumps(ret)
    return ret

@app.route("/v1/api/taglist", methods=['GET'])
def get_taglist():
    ret = es.get_taglist(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/attractions", methods=['GET'])
def get_attractions():
    ret = es.get_attractions(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/nearby", methods=['GET'])
def get_nearby():
    ret = es.get_nearby(request.args)
    ret = json.dumps(ret)
    return ret


@app.route("/v1/api/similar", methods=['GET'])
def get_similar():
    ret = es.get_similar(request.args)
    ret = json.dumps(ret)
    return ret


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=45678, threaded=True)
