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
def recall_candidates(user_id, user_topic_dict):
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
    candidates_feature_dict = get_features_by_strategy(strategies_keys.index(0), candidates_list, strategies_dict)

    # lda
    lda_dict = recall_items.recall_lda(user_id, 300)
    lda_list = lda_dict.keys()
    lda_feature_dict = get_features_by_strategy(strategies_keys.index(21), lda_list, strategies_dict)

    candidates_feature_dict.update(lda_feature_dict)
    candidates_dict.update(lda_dict)

    # kmeans


    return candidates_feature_dict, candidates_dict


def get_features_by_strategy(strategy_pos, strategy_list, strategies_dict):
    candidates_feature_dict = load(strategy_list, topic_num=5000)

    for candidate_item in strategy_list:
        copy_strategies_feature = strategies_dict.values()
        copy_strategies_feature.extend(candidates_feature_dict[candidate_item])
        candidates_feature_dict[candidate_item] = copy_strategies_feature

    for stratege_item in strategy_list:
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
    strategy_feature_dict = OrderedDict()

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

item_extractor = ItemExtractor()


def enumerate_article_attribute(attribute_name):
    pass


def enumerate_kmeans():
    save()  # save for explanation


def enumerate_item_topics(topic_num):
    topic_feature_vector = [0] * topic_num
    return topic_feature_vector


def save():
    pass


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


def get_kmeans(items_list):
    pass


def load(items_list, topic_num):
    items_feature_dict = {}
    feature_topic_vector = enumerate_item_topics(topic_num)

    for item in items_list:
        items_feature_dict[item] = [0]*len(feature_topic_vector)

    items_topic = get_item_topic(items_list)
    for item_topic in items_topic:
        items_feature_dict[item_topic[0]][item_topic[1]] = 1  # item_topic[2]

    return items_feature_dict  # item id, feature vector pair.


if __name__ == '__main__':
    print enumerate_article_pname()
