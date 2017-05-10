# -*- coding: utf-8 -*-
# @Time    : 16/12/23 下午3:25
# @Author  : liulei
# @Brief   : 
# @File    : produce_consume_nid.py
# @Software: PyCharm Community Edition

import requests
def push_nid_to_queue(nid):
    #lda_model处理
    lda_url = "http://127.0.0.1:9986/topic_model/predict_news_topic"
    lda_data = {}
    lda_data['nid'] = nid
    requests.get(url=lda_url, params=lda_data)

    #去除广告服务
    ads_url = "http://127.0.0.1:9997/news_process/RemoveAdsOnnid"
    ads_data = {}
    ads_data['nid'] = nid
    requests.get(url=ads_url, params=ads_data)


