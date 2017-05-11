# -*- coding: utf-8 -*-
# @Time    : 17/4/28 上午11:16
# @Author  : liulei
# @Brief   : 
# @File    : subject_queue.py
# @Software: PyCharm Community Edition
from redis import Redis
import json
import traceback
from util.logger import Logger
import os
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_produce = Logger('pruduce', os.path.join(real_dir_path,  'log/log_sub_produce.txt'))
sub_redis = Redis(host='localhost', port=6379)
sub_queue = 'subject_queue' #专题队列

def product_subject(sub):
    sub_redis.lpush(sub_queue, json.dumps(sub))
    logger_produce.info('produce sub: {}'.format(sub))

def consume_subject():
    from subject import generate_subject
    global sub_redis
    while True:
        try:
            ddd = sub_redis.brpop(sub_queue)
            print ddd
            sub = json.loads(sub_redis.brpop(sub_queue)[1])
            print sub
            generate_subject(sub)
            break

        except :
            traceback.print_exc()
            break
