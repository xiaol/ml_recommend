# -*- coding: utf-8 -*-

from util.postgres import postgres as pg
import datetime

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
    pass


def enumerate_article_attribute(attribute_name):
    pass


def construct_item_feature_matrix():
    pass


def load():
    return {1: [0, 1], 2: [1, 0], 3: [1, 0], 4: [0, 1]}   # item_id , feature vector


if __name__ == '__main__':
    print enumerate_article_pname()
