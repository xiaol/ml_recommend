# -*- coding: utf-8 -*-
# @Time    : 17/7/3 下午4:47
# @Author  : liulei
# @Brief   : 
# @File    : test2.py
# @Software: PyCharm Community Edition

from util.doc_process import get_postgredb_query
from util import simhash
import datetime

def get_hashval():
    sql = "select nid, hash_val from news_simhash where ctime > now() - interval '2 day'"
    t0 = datetime.datetime.now()
    conn, cursor = get_postgredb_query()
    cursor.execute(sql)
    rows = cursor.fetchall()
    print 'compare with {}'.format(len(rows))
    hashval = 3255685376439667788
    same = []
    for r in rows:
        if simhash.dif_bit(hashval, long(r[1])) <= 3:
            same.append(r[0])


    t1 = datetime.datetime.now()
    print 'it takes {} sec'.format((t1-t0).total_seconds())


