# -*- coding: utf-8 -*-

import os
import sys
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(path)

from util.postgres import postgres_read_only as pg
import datetime
from collections import OrderedDict


# TODO should cover only read  but not click users for cold start
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


def recall_candidates(boolean_users=True,  users_para=[33658617]):
    if boolean_users:
        users = users_para
    else:
        users = get_active_user('5 minutes')

    users_feature_dict, users_detail_dict, users_topic_dict = \
        user_extractor.load(users, 5000, '15 days')
    return users_feature_dict, users_detail_dict, users_topic_dict


def enumerate_user_os():
    os_type = {'android': 1, 'ios': 0}
    os_feature_dict = OrderedDict((v, 0) for k, v in os_type.iteritems())
    return os_feature_dict


def classify_user_active_time(time):
    pass


def enumerate_user_device_size(users):
    pass


def enumerate_network(users):
    pass


def enumerate_ctype(users):
    pass


def enumerate_province(users):
    pass


def enumerate_city(users):
    pass


def enumerate_area(users):
    pass


def enumerate_user_topic(topic_num):
    topic_feature_vector = [0]*topic_num
    return topic_feature_vector


def enumerate_user_kmeans(users):
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
    feature_dict = OrderedDict((r[0], 0) for r in rows)
    return feature_dict


def get_users_detail(users):
    sql = '''
               SELECT uid,brand,platform,os,os_version,device_size,network, ctype,
                province, city, area from user_device where uid in ({})
        '''
    rows = pg.query(sql.format(','.join(str(u) for u in users)))
    return rows


def get_users_topic(users, time_interval='15 days'):
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    sql = '''
            SELECT uid, topic_id, probability, model_v, create_time from user_topics_v2
            where create_time > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{}'  
            and uid in ({}) and model_v = '2017-04-07-10-49-37'
    '''
    rows = pg.query(sql.format(str_now, time_interval, ','.join(str(u) for u in users)))
    return rows


def get_user_kmeans(users):
    pass


class UserExtractor(object):

    feature_brand_dict = OrderedDict()

    def enumerate_user_brand(self, active_users):
        if not self.feature_brand_dict:
            self.feature_brand_dict = enumerate_user_attribute('brand', active_users)
        return self.feature_brand_dict

    def preprocess_users_feature(self, all_users):
        self.feature_brand_dict = self.enumerate_user_brand(all_users)

    def load(self, active_users, topic_num, topic_time_interval):
        users_feature_dict = {}
        if not self.feature_brand_dict:
            print 'Warning-----user brand dict init online---'
            self.feature_brand_dict = self.enumerate_user_brand(active_users)
        # Many users don't have device features, so need to initialize the feature dicts.
        for user in active_users:
            users_feature_dict[user] = [0]*len(self.feature_brand_dict)

        users_detail = get_users_detail(active_users)
        users_detail_dict = {}

        for user_detail in users_detail:
            # feature vector: brand platform, os, os_version,
            # device_size, network, ctype, province, city, area, user_history_topic, user_history_kmeans
            for k, v in self.feature_brand_dict.iteritems():
                self.feature_brand_dict[k] = 0

            if not user_detail:
                users_feature_dict[user_detail[0]] = self.feature_brand_dict.values()
                continue

            self.feature_brand_dict[user_detail[1]] = 1
            users_feature_dict[user_detail[0]] = self.feature_brand_dict.values()

            users_detail_dict[user_detail[0]] = user_detail[1:]
        '''
        ---------------------------------------------------------------------------------Ugly line
        '''
        topic_feature_offset = len(self.feature_brand_dict)
        feature_topic_vector = enumerate_user_topic(topic_num)
        users_topic_dict = {}

        for user in active_users:
            copy_feature_topic_vector = list(feature_topic_vector)
            users_feature_dict[user].extend(copy_feature_topic_vector)

        users_topic = get_users_topic(active_users, topic_time_interval)
        for user_topic in users_topic:

            if not user_topic:
                continue
            try:
                users_feature_dict[user_topic[0]][topic_feature_offset+user_topic[1]] = 1  # user_topic[2]
                users_topic_dict[user_topic[0]] = user_topic[1:]
            except:
                print '------hold-------'

        return users_feature_dict, users_detail_dict, users_topic_dict
        # return {1: [1, 0], 2: [0, 1], 3: [1, 0], 4: [0, 1]}   # user_id, user_feature vector

user_extractor = UserExtractor()

if __name__ == '__main__':
    users_test = get_active_user(time_interval='30 minutes')
    print user_extractor.load(users_test)
