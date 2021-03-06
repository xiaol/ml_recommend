# -*- coding: utf-8 -*-
# @Time    : 17/2/13 下午4:02
# @Author  : liulei
# @Brief   : for multi viewpoint
# @File    : sentence_hash_service.py
# @Software: PyCharm Community Edition
import sys
from multi_viewpoint import sentence_hash
import tornado
from tornado import web
from tornado import httpserver


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            #("/multi_vp/hash_sentence", HashSentence),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
    port = int(sys.argv[1])
    if port == 9965: #计算新闻的句子hash值
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(port)
        #sentence_hash.coll_sentence_hash()
        from redis_process import nid_queue
        #nid_queue.clear_sentence_simhash_queue()
        nid_queue.consume_nid_sentence_simhash(200)
    elif port == 9966:  #手动收集以前的新闻
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(port)
        #sentence_hash.coll_sentence_hash()
        from multi_viewpoint import sentence_hash
        sentence_hash.coll_sentence_hash()
    elif port == 9964:  #消费队列, 生成专题
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(port)
        from multi_viewpoint import subject_queue
        subject_queue.consume_subject()
    elif port == 9963:  #每隔1天, move一次sentence_hash
        from tornado import ioloop
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(port)
        from multi_viewpoint.sentence_hash import move_sentence_data
        #每一天执行一次数据迁移
        ioloop.PeriodicCallback(move_sentence_data, 24 * 3600 * 1000).start() #定时从点击表中取
        #move_sentence_data()
    elif port == 9962:  #手工工作
        from multi_viewpoint import subject
        subject.add_cover_to_sub()


    tornado.ioloop.IOLoop.instance().start()


