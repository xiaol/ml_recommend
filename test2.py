# -*- coding: utf-8 -*-
# @Time    : 17/7/3 下午4:47
# @Author  : liulei
# @Brief   : 
# @File    : test2.py
# @Software: PyCharm Community Edition

from util.doc_process import get_postgredb_query
from util import simhash
from sim_hash import sim_hash
import datetime

def get_hashval():
    sql = "select nid, hash_val from news_simhash where ctime > now() - interval '2 day'"
    conn, cursor = get_postgredb_query()
    cursor.execute(sql)
    rows = cursor.fetchall()
    t0 = datetime.datetime.now()
    print 'compare with {}'.format(len(rows))
    hashval = 3255685376439667788
    same = []
    for r in rows:
        if simhash.dif_bit(hashval, long(r[1])) <= 12:
            same.append(r[0])


    t1 = datetime.datetime.now()
    print len(same)
    print 'it takes {} sec'.format((t1-t0).total_seconds())


if __name__ == "__main__":
    n_list =[22152934, 22152935,22152936]
    sim_hash.cal_and_check_news_hash(n_list)

