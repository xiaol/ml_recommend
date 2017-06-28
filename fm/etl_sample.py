# -*- coding: utf-8 -*-

from util.postgres import postgres as pg
import datetime


def get_click_samples(active_users):
    sql = '''
        select * from newsrecommendclick 
                      where uid in ({})
    '''
    rows = pg.query(sql.format(','.join(str(u) for u in active_users)))
    return rows


def get_read_samples_by_pos(active_users, pos):
    sql = '''
        select uid, nid, readtime, logtype, logchid from newsrecommendread_{} 
                      where uid in ({})
    '''
    rows = pg.query(sql.format(pos, ','.join(str(u) for u in active_users)))
    return rows


def get_read_samples(active_users):
    users_dict = {}
    for user in active_users:
        pos = user % 100
        if users_dict[pos] is None:
            users_dict[pos] = {pos, [user]}
        else:
            users_dict[pos].append(user)

    read_samples = []
    for key_pos, user_list in users_dict.iteritems():
        read_samples.extend(get_read_samples_by_pos(user_list, key_pos))

    return read_samples


def news_recommend_read_helper(active_users):
    return active_users


def news_recommend_click():
    pass


def transform():
    pass

if __name__ == '__main__':
    print "hold"
