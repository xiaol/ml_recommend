# -*- coding: utf-8 -*-
# @Time    : 16/12/19 上午9:53
# @Author  : liulei
# @Brief   : 
# @File    : redis_lda.py
# @Software: PyCharm Community Edition
from graphlab_lda import topic_model
from graphlab_kmeans import kmeans
from sim_hash import sim_hash
from redis import Redis
import traceback
redis_inst = Redis(host='localhost', port=6379)
nid_queue = 'nid_queue_for_lda'


def produce_nid(nid):
    global redis_inst
    print 'produce ' + str(nid) + ' for lda'
    redis_inst.lpush(nid_queue, nid)


import datetime
def consume_nid(num):
    global redis_inst
    n = 0
    nid_list = []
    t0 = datetime.datetime.now()
    while True:
        nid = redis_inst.brpop(nid_queue)[1]
        if num:
            nid_list.append(nid)
            n += 1
            t1 = datetime.datetime.now()
            if n >=num or (t1 - t0).total_seconds() > 10:
                #topic_model.lda_predict_and_save(nid_list)
                topic_model.predict_topic_nids(nid_list)
                #for kmeans
                kmeans.kmeans_predict(nid_list)
                #for simhash
                sim_hash.cal_and_check_news_hash(nid_list)
                n = 0
                del nid_list[:]
                t0 = datetime.datetime.now()
        else:
            topic_model.predict_topic_nids([nid, ])


import json
user_click_queue = 'user_click_queue'
def produce_user_click(uid, nid, ctime):
    global redis_inst
    print 'produce user ' + str(uid) + ' ' + str(nid) + ' ' + ctime
    redis_inst.lpush(user_click_queue, json.dumps([uid, nid, ctime]))


#消费用户点击行为
def consume_user_click():
    global redis_inst
    while True:
        try:
            data = json.loads(redis_inst.brpop(user_click_queue)[1])
            uid = data[0]
            nid = data[1]
            ctime = data[2]
            print 'consum ' + str(uid) + ' ' + str(nid) + ' ' + 'begin'
            #for topic model
            topic_model.predict_click((uid, nid, ctime))
            #for kmeans
            kmeans.predict_click((uid, nid, ctime))
        except :
            traceback.print_exc()
            continue

