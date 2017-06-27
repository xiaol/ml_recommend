# -*- coding: utf-8 -*-

from util.postgres import postgres as pg
import datetime

def get_active_user():
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    sql = '''
            select uid from newsrecommendclick 
            where ctime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{} day'  
            group by uid HAVING "count"(*)>={}
        '''
    rows = pg.query(sql.format(str_now, 1, 1))

def enumerate_user_brand():
    pass


def enumerate_user_os_version():
    pass


def construct_user_feature_matrix():
    pass


def enumerate_user_attribute(attribute_name):
    pass


def load():
    return {1: [1, 0], 2: [0, 1], 3: [1, 0], 4: [0, 1]}   # user_id, user_feature vector

