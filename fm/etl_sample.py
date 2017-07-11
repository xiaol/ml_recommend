# -*- coding: utf-8 -*-

from util.postgres import postgres_read_only as pg
import datetime


def get_click_samples(active_users, time_interval):
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    sql = '''
        select  uid, nid, ctime, stime, logtype, logchid from newsrecommendclick 
            where ctime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{}'  
                      and uid in ({})
    '''
    rows = pg.query(sql.format(str_now, time_interval, ','.join(str(u) for u in active_users)))
    return rows


def get_read_samples_by_pos(active_users, pos, time_interval):
    nt = datetime.datetime.now()
    str_now = nt.strftime('%Y-%m-%d %H:%M:%S')
    sql = '''
        select uid, nid, readtime, logtype, logchid from newsrecommendread_{} 
            where readtime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{}'  
                      and uid in ({})
    '''
    rows = pg.query(sql.format(pos, str_now, time_interval, ','.join(str(u) for u in active_users)))
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


class SampleExtractor(object):
    time_dict = dict((i, 0) for i in range(24))

    def generate_time_feature(self, timestamp):
        for k, v in self.time_dict.iteritems():
            self.time_dict[k] = 0
        self.time_dict[timestamp.hour] = 1
        return self.time_dict.values()

sampleExtractor = SampleExtractor()


def news_recommend_read_helper(active_users):
    return active_users


if __name__ == '__main__':
    time_feature = sampleExtractor.generate_time_feature('2017-7-11 16:30:21')
    print "hold"
