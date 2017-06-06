# -*- coding: utf-8 -*-
# @Time    : 17/4/5 下午2:23
# @Author  : liulei
# @Brief   : 
# @File    : cf_service.py
# @Software: PyCharm Community Edition

import sys
import tornado
from tornado import httpserver
from tornado import web
from user_based_cf import data_process



if __name__ == '__main__':
    port = int(sys.argv[1])
    http_server = tornado.httpserver.HTTPServer(tornado.web.Application())
    http_server.listen(port)  # 同时提供手工处理端口
    if port == 9949:
        while True:
            data_process.get_user_topic_cf()


    tornado.ioloop.IOLoop.instance().start()
