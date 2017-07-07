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
from tornado import ioloop


if __name__ == '__main__':
    port = int(sys.argv[1])
    if len(sys.argv) >= 3:
        data_process.TEST_FLAG = bool(sys.argv[2])   #argv[2]是test标记, 1表示test，会打印信息，并保存中间数据
    http_server = tornado.httpserver.HTTPServer(tornado.web.Application())
    http_server.listen(port)  # 同时提供手工处理端口
    if port == 9949:
        while True:
            data_process.get_user_topic_cf()
    elif port == 9948:  #每隔1天, 清除一次数据
        #每一天执行一次数据迁移
        ioloop.PeriodicCallback(data_process.clear_data, 24 * 3600 * 1000).start() #定时从点击表中取


    tornado.ioloop.IOLoop.instance().start()
