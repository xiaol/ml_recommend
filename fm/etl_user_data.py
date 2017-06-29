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
    a_users = [r[0] for r in rows]
    return a_users


def enumerate_user_brand(active_users):
    return enumerate_user_attribute('brand', active_users)


def enumerate_user_os():
    pass


def classify_user_active_time(time):
    pass


def construct_user_feature_matrix():
    pass


# brand text, platform text, os 1:android, 0:ios, os_version text, device_size text,
# network int, ctype int, province text, city text, area text
def enumerate_user_attribute(attribute_name, active_users):
    user_device_sql = '''
                      select distinct {} 
                      from user_device 
                      where uid in ({})
                      '''
    rows = pg.query(user_device_sql.format(attribute_name, ','.join(str(u) for u in active_users)))
    feature_dict = dict((r[0], 0) for r in rows)
    return feature_dict


def get_active_users_detail(active_users):
    sql = '''
               SELECT uid,brand,platform,os,os_version,device_size,network, ctype,
                province, city, area from user_device where uid in ({})
        '''
    rows = pg.query(sql.format(','.join(str(u) for u in active_users)))
    return rows


def load(active_users, ):
    users_feature_dict = {}
    feature_brand_dict = enumerate_user_brand(active_users)
    users_detail = get_active_users_detail(active_users)
    for user_detail in users_detail:
        copy_feature_brand_dict = feature_brand_dict.copy()
        if not user_detail:
            users_feature_dict[user_detail[0]] = copy_feature_brand_dict.values()
            continue
        copy_feature_brand_dict[user_detail[1]] = 1
        users_feature_dict[user_detail[0]] = copy_feature_brand_dict.values()
    return users_feature_dict
    # return {1: [1, 0], 2: [0, 1], 3: [1, 0], 4: [0, 1]}   # user_id, user_feature vector

if __name__ == '__main__':
    users = get_active_user(time_interval='30 minutes')
    print load(users)
