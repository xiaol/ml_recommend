# -*- coding: utf-8 -*-
import os
import sys
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(path)

from util.postgres import postgres_read_only as pg
import datetime
from collections import OrderedDict


def get_positive_samples(active_users, time_interval):
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    # logtype 4 is topic channel
    sql = '''
        select  uid, nid, ctime, stime, logtype, logchid, chid from newsrecommendclick 
            where ctime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{}' 
            and logtype not in (4) and uid not in (0) and nid not in (0)
                      and uid in ({}) limit 4000
    '''
    rows = pg.query_dict_cursor(sql.format(str_now, time_interval, ','.join(str(u) for u in active_users)))
    return rows


def get_read_samples_by_pos(active_users, pos, time_interval):
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    sql = '''
        select uid, nid, readtime, logtype, logchid from newsrecommendread_{} 
            where readtime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{}'  
                      and uid in ({}) ORDER BY readtime limit {}'''
    condition = '''and uid not in (select uid from (select uid, count(1) as sc from newsrecommendread_{} 
                      where readtime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{}' 
                      and uid in ({}) group by uid ) as tb where sc > 1000)
    '''
    # TODO only support few user
    sql = sql.format(pos, str_now, time_interval, ','.join(str(u) for u in active_users),
                     len(active_users)*700*12)
    rows = pg.query_dict_cursor(sql)
    return rows


def get_read_samples(active_users, time_interval):
    users_dict = {}
    for user in active_users:
        pos = user % 100
        if pos not in users_dict:
            users_dict[pos] = [user]
        else:
            users_dict[pos].append(user)

    read_samples = []
    for key_pos, user_list in users_dict.iteritems():
        read_samples.extend(get_read_samples_by_pos(user_list, key_pos, time_interval))

    return read_samples


def get_hate_samples(users, time_interval='15 days'):
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    sql = '''
         select uid, nid, reason, ctime from hatenewslist
            where ctime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{}' 
            and uid in ({})
    '''
    rows = pg.query_dict_cursor(sql.format(str_now, time_interval, ','.join(str(u) for u in users)))
    return rows


class SampleExtractor(object):
    time_dict = OrderedDict((i, 0) for i in range(31))

    def generate_time_feature(self, timestamp):
        for k, v in self.time_dict.iteritems():
            self.time_dict[k] = 0
        self.time_dict[timestamp.hour] = 0.7
        self.time_dict[timestamp.today().weekday() + 24] = 0.5
        return self.time_dict.values()

sampleExtractor = SampleExtractor()


def news_recommend_read_helper(active_users):
    return active_users


if __name__ == '__main__':
    time_feature = sampleExtractor.generate_time_feature('2017-7-11 16:30:21')
    print "hold"
