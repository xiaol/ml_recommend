# -*- coding: utf-8 -*-
# @Time    : 16/11/21 下午4:34
# @Author  : liulei
# @Brief   : 
# @File    : redis.py
# @Software: PyCharm Community Edition

from redis import Redis
redis_inst = Redis(host='localhost', port=6379)
nid_queue = 'nid_queue'
def produce_nid(nid):
    global redis_inst
    print 'produce ' + nid
    redis_inst.lpush(nid_queue, nid)

#去除广告的主逻辑
def remove_ads_onnid_core(nid):
    from ads_remove import AdsExtract
    if len(nid) == 0:
        return
    pname, content_list = AdsExtract.get_para_on_nid(nid)
    #如果新闻不属于需要检查的源则返回
    print len(AdsExtract.ads_dict)
    if pname.decode('utf-8') not in dict(AdsExtract.ads_dict).keys():
        print 'not checked'
        return
    ret = AdsExtract.get_ads_paras(pname.decode('utf-8'), content_list)
    if len(ret) > 0:
        print 'begin to remove ' + nid
        AdsExtract.remove_ads(ret, nid)

def consume_nid():
    global redis_inst
    import requests
    while True:
        nid = redis_inst.brpop(nid_queue)[1]
        url = 'http://127.0.0.1:9999/news_process/RemoveAdsOnnidCore'
        data = {}
        data['nid'] = nid
        response = requests.get(url, params=data)


