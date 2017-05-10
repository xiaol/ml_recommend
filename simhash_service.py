# -*- coding: utf-8 -*-
# @Time    : 17/2/4 下午4:40
# @Author  : liulei
# @Brief   : 
# @File    : simhash_service.py
# @Software: PyCharm Community Edition
import json
import sys
import tornado
from tornado import web
from tornado import httpserver


if __name__ == '__main__':
    port = int(sys.argv[1])
    if port == 9969: #消费nid
        http_server = tornado.httpserver.HTTPServer(tornado.web.Application())
        http_server.listen(port) #同时提供手工处理端口
        from redis_process import nid_queue
        nid_queue.consume_nid_simhash(200)

    tornado.ioloop.IOLoop.instance().start()

