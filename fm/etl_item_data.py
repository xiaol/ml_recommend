# -*- coding: utf-8 -*-
import os
import sys
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(path)

from util.postgres import postgres_read_only as pg
import datetime
import recall_items
from collections import OrderedDict


# prepare the items for user recommend
def recall_candidates(item_extractor, user_id):
    """
    :users_topic_dict: lda

    user history lda + user history kmeans + cf + wilson + all channels' big picture/hot news + editor+
    specific topic(日本, 旅行), (动漫, 漫画)/ keyword / channel id
    :return:
    """
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    strategies_dict = item_extractor.enumerate_recommend_strategy()
    strategies_keys = strategies_dict.keys()

    # wilson
    candidates_dict = recall_items.recall_wilson_news(user_id, 3000)
    candidates_list = candidates_dict.keys()
    candidates_feature_dict = get_features_by_strategy(
        strategies_keys, candidates_dict, candidates_list, strategies_dict, item_extractor)

    # lda kmeans cf
    lkc_dict = recall_items.recall_lda_kmeans_cf(user_id, 500)
    lkc_list = lkc_dict.keys()
    lkc_feature_dict = get_features_by_strategy(
        strategies_keys, lkc_dict, lkc_list, strategies_dict,  item_extractor)

    candidates_feature_dict.update(lkc_feature_dict)
    candidates_dict.update(lkc_dict)

    # big img and video
    bv_dict = recall_items.recall_bigimg_video(user_id, 300)
    bv_list = bv_dict.keys()
    bv_feature_dict = get_features_by_strategy(
        strategies_keys, bv_dict, bv_list, strategies_dict, item_extractor)

    candidates_feature_dict.update(bv_feature_dict)
    candidates_dict.update(bv_dict)

    # hot news
    hn_dict = recall_items.recall_hot_news(user_id, 300)
    hn_list = hn_dict.keys()
    hn_feature_dict = get_features_by_strategy(
        strategies_keys, hn_dict, hn_list, strategies_dict, item_extractor)

    candidates_feature_dict.update(hn_feature_dict)
    candidates_dict.update(hn_dict)

    return candidates_feature_dict, candidates_dict


def get_features_by_strategy(strategies_keys, candidates_dict, strategy_list, strategies_dict, item_extractor):

    candidates_feature_dict = load(strategy_list, 5000, item_extractor)

    for candidate_item in strategy_list:
        copy_strategies_feature = strategies_dict.values()
        copy_strategies_feature.extend(candidates_feature_dict[candidate_item])
        candidates_feature_dict[candidate_item] = copy_strategies_feature

    for stratege_item in strategy_list:
        strategy_pos = strategies_keys.index(candidates_dict[stratege_item]['logtype'])
        candidates_feature_dict[stratege_item][strategy_pos] = 1

    return candidates_feature_dict


def enumerate_article_pname():
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    sql = '''
        select count(1) from  newspublisherlist_v2
    '''
    rows = pg.query(sql.format(str_now, 7))
    return rows[0][0]


def enumerate_article_editor_rank():
    pass


class ItemExtractor(object):
    '''
    strategy_feature_dict = OrderedDict()
    kmeans_feature_dict = OrderedDict()
    channel_feature_dict = OrderedDict()

    kmeans_size = 0
    '''
    def __init__(self):
        self.strategy_feature_dict = OrderedDict()
        self.kmeans_feature_dict = OrderedDict()
        self.channel_feature_dict = OrderedDict()
        self.kmeans_size = 0

    def enumerate_recommend_strategy(self):
        if self.strategy_feature_dict:
            return self.strategy_feature_dict
        # strategy = {'hot': 0, 'recommend': 1, 'wilson': 2, 'editor': 3, 'other': 4}
        logtype = {'wilson': 0, 'topic collection': 4, 'slide image news': 5, 'video': 6,
                   'local news': 7, 'channel hotnews': 13, 'baidu keyword': 11, 'comment news': 12, 'hotnews': 14,
                   'lda': 21, 'kmeans': 22, 'editor chosen hot news': 23, 'editor': 24,
                   'big image news': 25, 'related images':26, 'CF': 27, 'news in comment center': 28,
                   'channel big image news': 29, 'news in topic': 41, 'top rank': 100, }
        # TODO dismatch logtype
        self.strategy_feature_dict = OrderedDict((v, 0) for k,v in logtype.iteritems())

        return self.strategy_feature_dict

    def enumerate_kmeans(self):
        if self.kmeans_feature_dict:
            return self.kmeans_feature_dict, self.kmeans_size
        chnl_k_dict = OrderedDict({'财经': 20, '股票': 10, '故事': 20, '互联网': 20, '健康': 50, '军事': 20,
                                   '科学': 20, '历史': 30, '旅游': 20, '美食': 20, '美文': 20, '萌宠': 10,
                                   '汽车': 30, '时尚': 10, '探索': 10, '外媒': 30, '养生': 30, '影视': 10,
                                   '游戏': 30, '育儿': 20, '体育': 20, '娱乐': 20, '社会': 20, '科技': 12,
                                   '国际': 5, '美女': 1, '搞笑': 1, '趣图': 1, '风水玄学': 10, '本地': 20,
                                   '自媒体': 80, '奇闻': 10})
        for k,v in chnl_k_dict.iteritems():
            self.kmeans_feature_dict[k] = v
            self.kmeans_size += v
        return self.kmeans_feature_dict, self.kmeans_size

    def enumerate_channel(self):
        if self.channel_feature_dict:
            return self.channel_feature_dict
        self.channel_feature_dict = get_channel_list()
        return self.channel_feature_dict


def enumerate_article_attribute(attribute_name):
    pass


def enumerate_item_topics(topic_num):
    topic_feature_vector = [0] * topic_num
    return topic_feature_vector


def get_item_detail(items_list):
    sql = '''
        select * from newslist_v2 where nid in ({})
    '''
    rows = pg.query(sql.format(','.join(str(i) for i in items_list)))
    return rows


def get_item_topic(items_list, model_v='2017-04-07-10-49-37'):
    sql = '''
        select nid, topic_id, probability, model_v, ctime from news_topic_v2 
        where model_v= '{}' and nid in ({}) 
    '''
    rows = pg.query(sql.format(model_v, ','.join(str(i) for i in items_list)))
    return rows


def get_item_channel_and_kmeans(items_list, model_v='2017-06-27-14-59-59'):
    sql = '''
        select nid, model_v, ch_name, cluster_id, chid, ctime from news_kmeans 
        where model_v= '{}' and nid in ({})
    '''
    rows = pg.query_dict_cursor(sql.format(model_v, ','.join(str(i) for i in items_list)))
    return rows


def get_channel_list():
    channel_dict = OrderedDict()
    sql = "select id, cname from channellist_v2"
    rows = pg.query_dict_cursor(sql)
    for r in rows:
        channel_dict[r[1]] = r[0]
    return channel_dict


def load(items_list, topic_num, item_extractor):
    items_feature_dict = OrderedDict()

    if not items_list:
        return items_feature_dict

    # add topic feature
    feature_topic_vector = enumerate_item_topics(topic_num)
    topic_offset = len(feature_topic_vector)

    channel_feature_dict = item_extractor.enumerate_channel()
    channel_offest = len(channel_feature_dict)

    kmeans_feature_dict, kmeans_offset = item_extractor.enumerate_kmeans()

    for item in items_list:
        items_feature_dict[item] = [0]*(topic_offset + channel_offest + kmeans_offset)

    items_topic = get_item_topic(items_list)
    for item_topic in items_topic:
        items_feature_dict[item_topic[0]][item_topic[1]] = item_topic[2]*2

    # add kmeans feature
    # add channel feature
    items_channel_kmeans = get_item_channel_and_kmeans(items_list)
    channel_feature_list = channel_feature_dict.keys()
    for item_ck in items_channel_kmeans:
        items_feature_dict[item_ck['nid']][topic_offset + channel_feature_list.index(item_ck['ch_name'])]= 1

    for item_ck in items_channel_kmeans:
        kmeans_pos = 0
        for k,v in kmeans_feature_dict.iteritems():
            if item_ck['ch_name'] != k:
                kmeans_pos += kmeans_feature_dict[k]
            else:
                kmeans_pos += item_ck['cluster_id']
                break

        items_feature_dict[item_ck['nid']][topic_offset + channel_offest + kmeans_pos]= 1

    return items_feature_dict  # item id, feature vector pair.


if __name__ == '__main__':
    print enumerate_article_pname()
