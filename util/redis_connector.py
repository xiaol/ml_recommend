# coding: utf-8

""" 缓存相关, 目前使用的 redis """

from redis import from_url

__author__ = "Laite Sun"
__copyright__ = "Copyright 2016-2019, ShangHai Lie Ying"
__license__ = "Private"
__email__ = "sunlt@lieying.cn"
__date__ = "2017-03-1 15:05"

from conf import DEBUG
REDIS_URL = 'redis://ccd827d637514872:LYcache2015' \
            '@ccd827d637514872.m.cnhza.kvstore.aliyuncs.com:6379'

if DEBUG:
    redis_ali = from_url("localhost", db=1,  max_connections=30)
    # redis = from_url(REDIS_URL, db=1, max_connections=30)
else:
    redis_ali = from_url(REDIS_URL, db=1, max_connections=30)

