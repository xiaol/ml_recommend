# -*- coding: utf-8 -*-

from util.postgres import postgres as pg
import datetime


def recall_candidates():
    pass


def set_lda_topic_num(num=5000):
    num;pass


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


def enumerate_recommend_strategy():
    # strategy = {'hot': 0, 'recommend': 1, 'wilson': 2, 'editor': 3, 'other': 4}
    logtype = {'wilson':0, 'collection':4, 'slide image news':5, 'video':6, 'local news':7,
                'baidu keyword':11, 'comment news':12, 'lda': 21, 'kmeans':22, 'editor+machine':23,
               'editor': 24, 'big image news':25, 'related images':26, 'CF':27, 'news in topic':41,
               'top rank':100, }
    # TODO dismatch logtype
    strategy_feature_dict = dict((v, 0) for k,v in logtype.iteritems())

    return strategy_feature_dict


def enumerate_article_attribute(attribute_name):
    pass


def enumerate_kmeans():
    save()  # save for explanation


def construct_item_feature_matrix():
    pass


def save():
    pass


def get_item_detail(items_list):
    sql = '''
        select * from newslist_v2 where nid in ({})
    '''
    rows = pg.query(sql.format(','.join(str(i) for i in items_list)))
    return rows


def load(items_list):
    items_detail = get_item_detail(items_list)
    for item_detail in items_detail:
       pass # TODO
    return {1: [0, 1], 2: [1, 0], 3: [1, 0], 4: [0, 1]}   # item_id , feature vector


if __name__ == '__main__':
    print enumerate_article_pname()
