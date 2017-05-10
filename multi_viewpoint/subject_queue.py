# -*- coding: utf-8 -*-
# @Time    : 17/4/28 上午11:16
# @Author  : liulei
# @Brief   : 
# @File    : subject_queue.py
# @Software: PyCharm Community Edition
from redis import Redis
import json
import traceback


sub_redis = Redis(host='localhost', port=6379)
sub_queue = 'subject_queue' #专题队列

def product_subject(sub_nids):
    sub_redis.lpush(sub_queue, json.dumps(sub_nids))

def consume_subject():
    from subject import generate_subject
    global sub_redis
    while True:
        try:
            sub = json.loads(sub_redis.brpop(sub_queue)[1])
            generate_subject(sub)

        except :
            traceback.print_exc()
            continue
