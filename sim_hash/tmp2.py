# -*- coding: utf-8 -*-
# @Time    : 17/6/6 下午3:00
# @Author  : liulei
# @Brief   : 
# @File    : tmp2.py
# @Software: PyCharm Community Edition

from util import doc_process
from sim_hash import del_nid_of_fewer_comment
import os
from util.logger import Logger

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
tmp_logger2 = Logger('tmp_sim_hash2', os.path.join(real_dir_path, 'log/tmp_log2.txt'))
def delete_interval():
    sql = "select nid, same_nid from news_simhash_map where ctime > now() - interval '6 day'"
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for r in rows:
        del_nid_of_fewer_comment(r[0], r[1], tmp_logger2)

    cursor.close()
    conn.close()

