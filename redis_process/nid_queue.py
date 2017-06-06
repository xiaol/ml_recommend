# -*- coding: utf-8 -*-
# @Time    : 17/2/8 下午3:19
# @Author  : liulei
# @Brief   : 
# @File    : nid_queue.py
# @Software: PyCharm Community Edition

#from graphlab_lda import topic_model
from graphlab_lda import topic_model_model
from graphlab_kmeans import kmeans
from sim_hash import sim_hash
from multi_viewpoint import sentence_hash
from redis import Redis
import traceback
import json
import datetime
import logging
import os

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(real_dir_path + '/../base_service/log/log.txt')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

redis_inst = Redis(host='localhost', port=6379)
nid_queue = 'nid_queue'
lda_queue = 'lda_queue'
kmeans_queue = 'kmeans_queue'
simhash_queue = 'simhash_queue'
tmp_simhash_queue = 'tmp_simhash_queue'
sentence_simhash_queue = 'sentence_simhash_queue'
ads_queue = 'ads_queue'


def produce_nid(nid):
    global redis_inst
    #redis_inst.lpush(nid_queue, nid)
    redis_inst.lpush(lda_queue, nid)
    redis_inst.lpush(kmeans_queue, nid)
    redis_inst.lpush(simhash_queue, nid)
    redis_inst.lpush(ads_queue, nid)
    logger.info("produce {}, push it to queues.".format(nid))


'''
def consume_nid():
    global redis_inst
    while True:
        nid = redis_inst.brpop(nid_queue)[1]
        redis_inst.lpush(lda_queue, nid)
        redis_inst.lpush(kmeans_queue, nid)
        redis_inst.lpush(simhash_queue, nid)
        redis_inst.lpush(ads_queue, nid)
'''



def consume_nid_lda(num=1):
    global redis_inst
    n = 0
    nid_list = []
    t0 = datetime.datetime.now()
    while True:
        nid = redis_inst.brpop(lda_queue)[1]
        nid_list.append(nid)
        n += 1
        t1 = datetime.datetime.now()
        if n >=num or (t1 - t0).total_seconds() > 10:
            #topic_model.predict_topic_nids(nid_list)
            topic_model_model.predict_nids(nid_list)
            n = 0
            del nid_list[:]
            t0 = datetime.datetime.now()


def consume_nid_kmeans(num=1):
    global redis_inst
    n = 0
    nid_list = []
    t0 = datetime.datetime.now()
    while True:
        nid = redis_inst.brpop(kmeans_queue)[1]
        nid_list.append(nid)
        n += 1
        t1 = datetime.datetime.now()
        if n >=num or (t1 - t0).total_seconds() > 10:
            kmeans.kmeans_predict(nid_list)
            n = 0
            del nid_list[:]
            t0 = datetime.datetime.now()


def consume_nid_simhash(num=1):
    global redis_inst
    n = 0
    nid_list = []
    t0 = datetime.datetime.now()
    while True:
        nid = redis_inst.brpop(simhash_queue)[1]
        nid_list.append(nid)
        n += 1
        t1 = datetime.datetime.now()
        if n >=num or (t1 - t0).total_seconds() > 0.1:
            sim_hash.cal_and_check_news_hash(nid_list)
            for i in nid_list:
                redis_inst.lpush(sentence_simhash_queue, i)
            n = 0
            del nid_list[:]
            t0 = datetime.datetime.now()


def consume_nid_sentence_simhash(num=1):
    global redis_inst
    n = 0
    nid_list = []
    t0 = datetime.datetime.now()
    while True:
        '''
        if not redis_inst.brpop(sentence_simhash_queue):
            print 'non sentence_hash'
            break
        '''
        nid = redis_inst.brpop(sentence_simhash_queue)[1]
        nid_list.append(nid)
        n += 1
        t1 = datetime.datetime.now()
        if n >= num or (t1 - t0).total_seconds() > 10:
            sentence_hash.coll_sentence_hash_time(nid_list)
            n = 0
            del nid_list[:]
            t0 = datetime.datetime.now()


def consume_nid_ads():
    global redis_inst
    import requests
    while True:
        nid = redis_inst.brpop(ads_queue)[1]
        url = 'http://127.0.0.1:9999/news_process/RemoveAdsOnnidCore'
        data = {}
        data['nid'] = nid
        response = requests.get(url, params=data)


user_click_queue_lda = 'user_click_queue_lda'
user_click_queue_kmeans = 'user_click_queue_kmeans'
def produce_user_click_lda(uid, nid, ctime):
    global redis_inst
    print 'produce user ' + str(uid) + ' ' + str(nid) + ' ' + ctime
    redis_inst.lpush(user_click_queue_lda, json.dumps([uid, nid, ctime]))


def produce_user_click_kmeans(uid, nid, ctime):
    global redis_inst
    print 'produce user ' + str(uid) + ' ' + str(nid) + ' ' + ctime
    redis_inst.lpush(user_click_queue_kmeans, json.dumps([uid, nid, ctime]))

def consume_user_click_kmeans():
    global redis_inst
    while True:
        try:
            data = json.loads(redis_inst.brpop(user_click_queue_kmeans)[1])
            uid = data[0]
            nid = data[1]
            ctime = data[2]
            kmeans.predict_click((uid, nid, ctime))
        except :
            traceback.print_exc()
            continue

#消费用户点击行为
def consume_user_click_lda():
    global redis_inst
    while True:
        try:
            data = json.loads(redis_inst.brpop(user_click_queue_lda)[1])
            uid = data[0]
            nid = data[1]
            ctime = data[2]
            topic_model_model.predict_click((uid, nid, ctime))
        except :
            traceback.print_exc()
            continue

def get_old_clicks():
    global redis_inst
    clicks = []
    while True:
        try:
            d = redis_inst.rpop(user_click_queue_lda)
            if not d:
                break
            data = json.loads(d)
            uid = data[0]
            nid = data[1]
            ctime = data[2]
            clicks.append((uid, nid, ctime))
        except:
            raise
    return clicks

def clear_queue_kmeans():
    global redis_inst
    while True:
        nid = redis_inst.rpop(kmeans_queue)
        if not nid:
            break


def clear_kmeans_queue_click():
    global redis_inst
    while True:
        click = redis_inst.rpop(user_click_queue_kmeans)
        if not click:
            break

def clear_queue_lda():
    global redis_inst
    while True:
        nid = redis_inst.rpop(lda_queue)
        if not nid:
            break


def clear_lda_queue_click():
    global redis_inst
    while True:
        click = redis_inst.rpop(user_click_queue_lda)
        if not click:
            break

def clear_sentence_simhash_queue():
    global redis_inst
    while True:
        click = redis_inst.rpop(sentence_simhash_queue)
        if not click:
            break


def tmp_simhash_queue_produce(nids):
    global redis_inst
    #redis_inst.lpush(nid_queue, nid)
    for nid in nids:
        redis_inst.lpush(tmp_simhash_queue, nid)

def tmp_consume_nid_simhash(num=200):
    try:
        global redis_inst
        n = 0
        nid_list = []
        while True:
            nid = redis_inst.brpop(tmp_simhash_queue)[1]
            nid_list.append(nid)
            n += 1
            if n >= num:
                sim_hash.cal_and_check_news_hash(nid_list)
                n = 0
                del nid_list[:]
        print 'tmp_consume_nid_simhash finished!'
    except:
        traceback.print_exc()


def push_to_simhash(nids):
    global redis_inst
    for nid in nids:
        redis_inst.lpush(simhash_queue, nid)
