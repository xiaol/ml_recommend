# -*- coding: utf-8 -*-

from util.postgres import postgres as pg
import datetime

# TODO should cover unread users for cold start
def get_active_user(time_interval='1 day', click_times=1):
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    sql = '''
            select uid from newsrecommendclick 
            where ctime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{}'  
            group by uid HAVING "count"(*)>={}
        '''
    rows = pg.query(sql.format(str_now, time_interval, click_times))
    active_users = [r[0] for r in rows]
    return active_users


def enumerate_user_brand():
    pass


def enumerate_user_os_version():
    pass


def classify_user_active_time(time):
    pass


def construct_user_feature_matrix():
    pass


# brand text, platform text, os 1:android, 0:ios, os_version text, device_size text,
# network int, ctype int, province text, city text, area text
def enumerate_user_attribute(attribute_name, active_users):
    user_device_sql = '''
                      select count(distinct {}) 
                      from user_device 
                      where uid in ({})
                      '''
    rows = pg.query(user_device_sql.format(attribute_name, ','.join(str(u) for u in active_users)))
    return rows[0][0]


def load(active_users):
    return {1: [1, 0], 2: [0, 1], 3: [1, 0], 4: [0, 1]}   # user_id, user_feature vector

if __name__ == '__main__':
    active_users = get_active_user()
    print enumerate_user_brand()