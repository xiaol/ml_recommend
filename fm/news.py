# -*- coding: utf-8 -*-


class News(object):
    # TODO NewsFeedResponse for redis
    nid = 0
    docid = ''
    title = ''
    ptime = '' # '2017-07-09 20:50:14'
    pname = ''
    purl = ''
    channel = 0  # chid
    concern = 0
    un_concern = 0
    comment = 0
    style = 0
    imgs = ''
    rtype = 0
    adimpression = ''
    icon = ''
    videourl = ''
    thumbnail = ''
    duration = 0
    adresponse = None  # adResponse
    logtype = 0
    logchid = 0
    tags = ''
    playtimes = 0 # clicktimes

    def __init__(self):
        pass

    @classmethod
    def format_news(cls, news_item):
        news_item['playtimes'] = news_item['clicktimes']
        news_item['channel'] = news_item['chid']

        del news_item['clicktimes']
        del news_item['chid']



