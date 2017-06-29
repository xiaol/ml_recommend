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
    elif port == 9968: #添加手工处理旧数据
        from sim_hash import tmp_sim_hash
        nids = tmp_sim_hash.find_old_news()
        from redis_process import nid_queue
        nid_queue.tmp_simhash_queue_produce(nids)
    elif port == 9967: #添加手工处理旧数据
        from redis_process import nid_queue
        #nid_queue.tmp_consume_nid_simhash()
        from sim_hash import tmp2
        nids = tmp2.coll_news()
        print 'coll finished!'
        nid_queue.push_to_simhash(nids)
        print '--------finished'
    elif port == 9966: #计算hash
        from sim_hash import tmp_sim_hash
        #tmp_sim_hash.cal_simhash_old()
        tmp_sim_hash.check_and_remove_news()
    elif port == 9970:
        http_server = tornado.httpserver.HTTPServer(tornado.web.Application())
        http_server.listen(port) #同时提供手工处理端口
        from multi_viewpoint import subject_queue
        subject_queue.consume_simhash2()



    tornado.ioloop.IOLoop.instance().start()

