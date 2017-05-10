# -*- coding: utf-8 -*-
# @Time    : 16/8/2 下午5:27
# @Author  : liulei
# @Brief   : 
# @File    : startService.py
# @Software: PyCharm Community Edition

import json
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.httpserver
import tornado.netutil
import tornado.gen
import sys
import operator
import classification.match_name as match_name
from classification import DocPreProcess
from classification import FeatureSelection
from classification import FeatureWeight
from svm_module import SVMClassify
#from classification.MongodbProcess import checkAds
from classification import AdsExtract

class FetchContent(tornado.web.RequestHandler):
    #@tornado.gen.coroutine
    def get(self):
        type = self.get_argument('type', None)
        txt = self.get_argument('txt', None)
        name_instance = match_name.NameFactory(type)
        if name_instance:
            #name = yield name_instance.getArticalTypeList(txt)
            name = name_instance.getArticalTypeList(txt)
            if name:
                ret = {'bSuccess': True, 'name': name}
                self.write(json.dumps(ret))
            else:
                ret = {'bSuccess': False, 'msg': 'Can not get any {0} name from the txt'.format(type)}
                self.write(json.dumps(ret))

class NewsClassifyOnNids(tornado.web.RequestHandler):
    #@tornado.gen.coroutine
    def post(self):
        nids = self.get_body_arguments('nids')
        #texts = self.get_body_arguments('texts')
        #text = DocPreProcess.getTextOfNewsNid(nid)
        #res = yield SVMClassify.svmPredictOneText(text)
        #res = SVMClassify.svmPredictOneText(texts)
        ret = SVMClassify.svmPredictNews2(nids)
        self.write(json.dumps(ret))

class NewsClassifyOnSrcid(tornado.web.RequestHandler):
    #@tornado.gen.coroutine
    def post(self):
        srcid = self.get_argument('srcid', None)
        category = self.get_argument('category', None)
        #nids = self.get_arguments('nids')
        nids = self.get_body_arguments('nids')
        #texts = self.get_arguments('texts')
        texts = self.get_body_arguments('texts')
        #ret = yield SVMClassify.svmPredictNews(srcid, nids, texts, category)
        ret = SVMClassify.svmPredictNews(nids, texts, srcid, category)
        self.write(json.dumps(ret))

#按照chennel id. 主要用於测试自媒体、点集、奇闻
class NewsClassifyOnChid(tornado.web.RequestHandler):
    #@tornado.gen.coroutine
    def post(self):
        chid = self.get_argument('chid', None)
        #nids = self.get_arguments('nids')
        nids = self.get_body_arguments('nids')
        #texts = self.get_arguments('texts')
        texts = self.get_body_arguments('texts')
        #ret = yield SVMClassify.svmPredictNews(chid, nids, texts)
        ret = SVMClassify.svmPredictNews(nids, texts, chid)
        self.write(json.dumps(ret))

class NewsAdsExtract(tornado.web.RequestHandler):
    def post(self):
        ads_raw_data_file = './result/ads_raw_data.txt'
        data = ''
        with open(ads_raw_data_file, 'r') as f:
            data = f.read()
        d = json.loads(data.encode('utf-8'))
        result = AdsExtract.extract_ads(d)
        #self.write(json.dumps(result))

class GetModifiedWechatNames(tornado.web.RequestHandler):
    def get(self):
        self.write(json.dumps(list(AdsExtract.modify_dict)))

class GetCheckedNames(tornado.web.RequestHandler):
    def post(self):
        self.write(json.dumps(AdsExtract.get_checked_name()))

class GetAdsOfOneWechat(tornado.web.RequestHandler):
    def post(self):
        name = self.get_body_argument('name')
        self.write(json.dumps(AdsExtract.get_ads_of_one_wechat(name)))

class ModifyNewsAdsResults(tornado.web.RequestHandler):
    def post(self):
        modify_type = self.get_body_argument('modify_type').encode('utf-8')
        modify_data = self.get_body_argument('modify_data').encode('utf-8')
        AdsExtract.modify_ads_results(modify_type, modify_data)

class SaveAdsModify(tornado.web.RequestHandler):
    def get(self):
        AdsExtract.save_ads_modify()
        #清空保存了修改微信号的set
        AdsExtract.modify_dict.clear()

class NewsAdsExtractOnnid(tornado.web.RequestHandler):
    def post(self):
        pname = self.get_body_argument('pname')
        content_list = json.loads(self.get_body_argument('contents'))
        response = AdsExtract.get_ads_paras(pname, content_list)
        self.write(json.dumps(response))


class Test(tornado.web.RequestHandler):
    def post(self):
        import requests
        nid_list = [9681927, 9681188]
        nid_chanl_list = {}
        print 'predict ---- 自媒体和点集 ' + str(len(nid_list))
        url = "http://127.0.0.1:9993/ml/newsClassifyOnNids"
        data = {}
        data['nids'] = nid_list
        response = requests.post(url, data=data)
        print response
        print type(response.content)
        print response.content
        print json.loads(response.content)
        self.write(json.dumps(response.content))

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/ml/fetchContent", FetchContent),
            (r"/ml/newsClassifyOnNids", NewsClassifyOnNids),
            (r"/ml/newsClassifyOnSrcid", NewsClassifyOnSrcid),
            (r"/ml/newsClassifyOnChid", NewsClassifyOnChid),
            (r"/ml/NewsAdsExtract", NewsAdsExtract),
            (r"/ml/NewsAdsExtractOnnid", NewsAdsExtractOnnid),
            (r"/ml/GetAdsOfOneWechat", GetAdsOfOneWechat),
            (r"/ml/GetCheckedNames", GetCheckedNames),
            (r"/ml/ModifyNewsAdsResults", ModifyNewsAdsResults),
            (r"/ml/SaveAdsModify", SaveAdsModify),
            (r"/ml/GetModifiedWechatNames", GetModifiedWechatNames),
            (r"/ml/test", Test),

        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == "__main__":
    print 'doc process'
    #DocPreProcess.docPreProcess()
    print 'feature select'
    #FeatureSelection.featureSelect()
    print 'feature weight'
    port = sys.argv[1]
    SVMClassify.set_news_predict_svm_file(port)
    #FeatureWeight.featureWeight()
    SVMClassify.getModel()
    print 'get model finishes!'
    #SVMClassify.svmPredictOnSrcid(3874)

    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(port)
    tornado.ioloop.IOLoop.instance().start()

