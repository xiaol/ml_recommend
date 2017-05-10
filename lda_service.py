# -*- coding: utf-8 -*-
# @Time    : 16/12/1 上午11:10
# @Author  : liulei
# @Brief   : 
# @File    : lda_service.py
# @Software: PyCharm Community Edition
import json
import sys

import tornado
from tornado import web
from tornado import httpserver
from tornado import ioloop
import traceback

#from graphlab_lda import redis_lda


class CollNews(tornado.web.RequestHandler):
    def get(self):
        from graphlab_lda import topic_model_doc_process
        num_per_chanl = int(self.get_argument('num'))
        topic_model_doc_process.coll_news_for_channles(num_per_chanl)


class CreateModels(tornado.web.RequestHandler):
    def get(self):
        from graphlab_lda import topic_model
        topic_model.create_models()


class PredictOnNid(tornado.web.RequestHandler):
    def get(self):
        nid = int(self.get_argument('nid'))
        from graphlab_lda import topic_model
        res = topic_model.lda_predict(nid)
        self.write(json.dumps(res))

#对外提供的接口。新闻入库后就执行
class PredictNewsTopic(tornado.web.RequestHandler):
    def get(self):
        nid = int(self.get_argument('nid'))
        #nid_queue.produce_nid(nid)  #由base_service完成

#预测单次点击事件
class PredictOneClick(tornado.web.RequestHandler):
    def get(self):
        try:
            uid = int(self.get_argument('uid'))
            nid = int(self.get_argument('nid'))
            ctime = self.get_argument('ctime')
            from graphlab_lda import topic_model
            topic_model.predict_user_topic_core(uid, nid, ctime)
        except :
            traceback.print_exc()



#对外提供的接口。对用户点击行为进行预测. 数据写入队列
class PredictUserTopic(tornado.web.RequestHandler):
    def post(self):
        clicks = json.loads(self.get_body_argument('clicks'))
        #uid = int(self.get_argument('uid'))
        #nid = int(self.get_argument('nid'))
        #ctime = self.get_argument('ctime')
        #from graphlab_lda import redis_lda
        for click in clicks:
            #redis_lda.produce_user_click(click[0], click[1], click[2])
            from redis_process import nid_queue
            nid_queue.produce_user_click(click[0], click[1], click[2])


class PredictOnNidAndSave(tornado.web.RequestHandler):
    def get(self):
        try:
            nid = int(self.get_argument('nid'))
            from graphlab_lda import topic_model
            topic_model.lda_predict_and_save(nid)
        except:
            traceback.print_exc()


class LoadModels(tornado.web.RequestHandler):
    def get(self):
        models_dir = self.get_argument('dir')
        from graphlab_lda import topic_model
        topic_model.load_models(models_dir)


#手动添加一些新闻进行预测。用于topic model启动时使用
class ProuceNewsTopicManual(tornado.web.RequestHandler):
    def get(self):
        num = self.get_argument('num')
        from graphlab_lda import topic_model
        topic_model.produce_news_topic_manual(num)


#根据用户的点击预测其感兴趣的话题,入库
class CollectUserTopic(tornado.web.RequestHandler):
    def get(self):
        from graphlab_lda import topic_model
        topic_model.get_user_topics()


class ProduceNewsApplication(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/predict_news_topic", PredictNewsTopic), #放入新闻队列
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)


class ProduceClickEventApplication(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/predict_clicks", PredictUserTopic),#可以将批量点击放入队列
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)



class ConsumeClickEventApplication(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/predict_one_click", PredictOneClick), #处理单次点击
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)

#用于手工的一些接口
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/coll_news", CollNews),
            ("/topic_model/create_models", CreateModels),
            ("/topic_model/predict_on_nid", PredictOnNid),
            #("/topic_model/get_topic_on_nid", PredictOnNidAndSave), #消费新闻
            ("/topic_model/load_models", LoadModels),
            ("/topic_model/produce_news_topic_manual", ProuceNewsTopicManual),
            ("/topic_model/get_user_topic", CollectUserTopic),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)

class CollNews2(tornado.web.RequestHandler):
    def get(self):
        from graphlab_lda.data_process import coll_news
        coll_news()


class CreateSaveModel(tornado.web.RequestHandler):
    def get(self):
        from graphlab_lda.topic_model_model import create_topic_model
        create_topic_model()


class PredictNewsTopic2(tornado.web.RequestHandler):
    def get(self):
        nids = self.get_arguments('nid')  #返回的nid是
        nids = map(int, nids)
        from graphlab_lda import topic_model_model
        res = topic_model_model.predict_nids(nids)
        self.write(json.dumps(res))

class CollectUserTopic2(tornado.web.RequestHandler):
    def get(self):
        model_v = '2017-03-20-17-33-53'
        uid = 11111
        nid = 5459927
        time_str = '2017-03-20 15:21:20'
        click = [uid, nid, time_str]
        from graphlab_lda import topic_model_model
        topic_model_model.predict_click(model_v, click)


#处理队列中的点击
class DealOldClicks(tornado.web.RequestHandler):
    def get(self):
        try:
            print '-------------deal old clicks begin!-------------'
            from redis_process import nid_queue
            clicks = nid_queue.get_old_clicks()
            print '----------------len = {}----------------'.format(str(len(clicks)))
            nids = [c[1] for c in clicks]
            from graphlab_lda import topic_model_model
            topic_model_model.deal_old_nids(nids)
            print '-------------deal old clicks predict nids finished!-------------'
            topic_model_model.predict_clicks(clicks)
            print '-------------deal old clicks finish!-------------'
        except:
            traceback.print_exc()


#处理库中两天内的点击
class DealOldClicks2(tornado.web.RequestHandler):
    def get(self):
        try:
            print '-------------deal old clicks begin!-------------'
            from graphlab_lda import topic_model_model
            s = "select uid, nid, ctime from newsrecommendclick where ctime > now() - interval '30 day'"
            from util import doc_process
            conn, cursor = doc_process.get_postgredb_query()
            cursor.execute(s)
            rows = cursor.fetchall()
            clicks = tuple(rows)
            topic_model_model.predict_clicks(clicks)
            print '-------------deal old clicks finish!-------------'
        except:
            traceback.print_exc()


#更新LDA版本后,收集部分旧news和clicks
class DealOldNewsClick(tornado.web.RequestHandler):
    def get(self):
        try:
            print '----deal old news and click----'
            from graphlab_lda import topic_model_model
            from redis_process import nid_queue
            from util import doc_process
            conn, cursor = doc_process.get_postgredb_query()
            nid_queue.clear_queue_click()
            nid_queue.clear_queue_lda() #清空旧nid
            s_new = "select nid from newslist_v2 where ctime > now() - interval '10 day' and chid not in (28, 23, 21, 44) and state=0"
            cursor.execute(s_new)
            rows = cursor.fetchall()
            nids = []
            for r in rows:
                nids.append(r[0])
            l = len(nids)

            if len(nids) < 1000:
                topic_model_model.predict_nids(nids)
            else:
                n = 0
                while (n + 1000) < len(nids):
                    topic_model_model.predict_nids(nids[n:n+1000])
                    n += 1000
                    print ('{} of {} finished!'.format(n, l))
                topic_model_model.predict_nids(nids[n-1000:len(nids)])

            print '    ----- finish to predict news, begin to predict click-----'

            s_click = "select uid, nid, ctime from newsrecommendclick where (ctime > now() - interval '10 day') and (ctime < now() - interval '1.5 day') "
            cursor.execute(s_click)
            clicks = tuple(cursor.fetchall())
            topic_model_model.predict_clicks(clicks)
            print '----------- finish to predict clicks--------'

            conn.close()
        except:
            traceback.print_exc()

# 用于手工的一些接口
class Application2(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/coll_news2", CollNews2),
            ("/topic_model/create_models2", CreateSaveModel),
            ("/topic_model/predict_on_nid2", PredictNewsTopic2),
            ("/topic_model/get_user_topic2", CollectUserTopic2),
            ("/topic_model/deal_old_clicks", DealOldClicks),
            ("/topic_model/deal_old_clicks2", DealOldClicks2),
            ("/topic_model/deal_old_news_click", DealOldNewsClick)
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
    if port == 9986:  #新闻入库后将nid加入到队列中,对外提供的接口  #不再需要。 在base_service.py中同一入队列
        http_server = tornado.httpserver.HTTPServer(ProduceNewsApplication())
        http_server.listen(port)
    elif port == 9987: #包含手工的一些接口和新闻的消费逻辑
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(port)
    elif port == 9988:#消费新闻队列数据
        http_server = tornado.httpserver.HTTPServer(EmptyApp())
        http_server.listen(port) #同时提供手工处理端口
        #topic_model.load_newest_models()
        from redis_process import nid_queue
        nid_queue.consume_nid_lda(200)
        #redis_lda.consume_nid(200)
    elif port == 9989: #用户点击事件入队列
        from graphlab_lda.timed_task import get_clicks_5m, period
        ioloop.PeriodicCallback(get_clicks_5m, period * 1000).start() #定时从点击表中取
        http_server = tornado.httpserver.HTTPServer(ProduceClickEventApplication())
        http_server.listen(port) #同时提供手工处理端口
    elif port == 9990: #消费用户点击逻辑进程。
        http_server = tornado.httpserver.HTTPServer(EmptyApp())
        http_server.listen(port) #同时提供手工处理端口
        from redis_process import nid_queue
        nid_queue.consume_user_click_lda()
    elif port == 9985:
        http_server = tornado.httpserver.HTTPServer(Application2())
        http_server.listen(port)
        #from graphlab_lda import data_process
        #data_process.coll_news()

    tornado.ioloop.IOLoop.instance().start()


