# -*- coding: utf-8 -*-
# @Time    : 17/1/4 下午3:22
# @Author  : liulei
# @Brief   : 
# @File    : kmeans_service.py.py
# @Software: PyCharm Community Edition

import json
import sys
import tornado
from tornado import web
from tornado import httpserver
from tornado import ioloop

class CreateKmeansModel(tornado.web.RequestHandler):
    def get(self):
        from graphlab_kmeans import kmeans_for_update
        #kmeans.create_kmeans_models()
        kmeans_for_update.create_new_kmeans_model()

class PredictKmeans(tornado.web.RequestHandler):
    def get(self):
        nids = self.get_arguments('nid')
        from graphlab_kmeans import kmeans_for_update
        res = kmeans_for_update.kmeans_predict(nids)
        self.write(json.dumps(res))


class RandomPredictKmeans(tornado.web.RequestHandler):
    def get(self):
        from graphlab_kmeans import kmeans_for_update
        res = kmeans_for_update.random_predict_nids()
        self.write(json.dumps(res))

class PredictClick(tornado.web.RequestHandler):
    def get(self):
        uid = 1
        nid = 10682265
        time_str = '2016-12-20 07:11:53'
        from graphlab_kmeans import kmeans_for_update
        kmeans.predict_click((uid, nid, time_str))



class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/kmeans/createmodel", CreateKmeansModel),
            ("/kmeans/predict_nids", PredictKmeans),
            ("/kmeans/predict_click", PredictClick),
            ("/kmeans/predict_random_nids", RandomPredictKmeans),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)

#空白应用。 可以起到占用端口,防止某个服务被反复启动
class EmptyApp(tornado.web.Application):
    def __init__(self):
        handlers = [

        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
    port = int(sys.argv[1])
    if port == 9100:
        http_server = tornado.httpserver.HTTPServer(Application())  #包含训练新模型及测试新模型
        http_server.listen(port) #同时提供手工处理端口
    elif port == 9980: #废弃 2017.06.23
        from graphlab_kmeans import kmeans
        kmeans.updateModel()
    elif port == 9981:  #click 入队列
        from graphlab_kmeans.timed_task import get_clicks_5m, period
        ioloop.PeriodicCallback(get_clicks_5m, period * 1000).start() #定时从点击表中取
        http_server = tornado.httpserver.HTTPServer(EmptyApp())
        http_server.listen(port) #同时提供手工处理端口
    elif port == 9979: #消费nid
        http_server = tornado.httpserver.HTTPServer(tornado.web.Application())
        http_server.listen(port) #同时提供手工处理端口
        from redis_process import nid_queue
        nid_queue.consume_nid_kmeans(200)
    elif port == 9978: #消费click队列
        http_server = tornado.httpserver.HTTPServer(EmptyApp())
        http_server.listen(port) #同时提供手工处理端口
        from redis_process import nid_queue
        nid_queue.consume_user_click_kmeans()
    elif port == 9977:  #更新模型并重启nid消费,启动9977后不用再启动9979
        http_server = tornado.httpserver.HTTPServer(tornado.web.Application())
        http_server.listen(port) #同时提供手工处理端口
        from graphlab_kmeans import kmeans
        kmeans.deal_old_news_clicks(3, deal_news=True, deal_click=True)
        from redis_process import nid_queue
        nid_queue.consume_nid_kmeans(200)
    elif port == 9976:  #离线计算旧数据
        http_server = tornado.httpserver.HTTPServer(EmptyApp())
        http_server.listen(port) #同时提供手工处理端口
        from graphlab_kmeans import kmeans
        kmeans.deal_old_news_clicks(3, deal_news=True, deal_click=True) #计算七天数据
    elif port == 9975: #计算特定频道的数据
        http_server = tornado.httpserver.HTTPServer(EmptyApp())
        http_server.listen(port) #同时提供手工处理端口
        from graphlab_kmeans import kmeans_for_chnl
        kmeans_for_chnl.predict_chnl_news('健康')
        kmeans_for_chnl.predict_chnl_news('养生')


    tornado.ioloop.IOLoop.instance().start()
