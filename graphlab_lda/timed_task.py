# -*- coding: utf-8 -*-
# @Time    : 16/12/23 上午10:37
# @Author  : liulei
# @Brief   : 定时任务
# @File    : timed_task.py
# @Software: PyCharm Community Edition

from util.doc_process import get_postgredb
from redis_process import nid_queue
from data_process import channel_for_topic_dict
import datetime
from datetime import timedelta
from util import logger
import os
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_9989 = logger.Logger('process9989_3',  os.path.join(real_dir_path,  'log/log_9989_3.txt'))
#定义取用户点击的循环周期
period = 3
#click_sql = "select uid, nid, ctime from newsrecommendclick where ctime > now() - INTERVAL '5 minute'"
click_sql = "select c.uid, c.nid, c.ctime from newsrecommendclick c \
inner join newslist_v2 nl  on c.nid=nl.nid \
INNER JOIN channellist_v2 cl on nl.chid = cl.id \
where cname in ({0}) and c.ctime > '{1}' order by c.ctime"
#where cname in ({0}) and c.ctime > now() - INTERVAL '{1} second' and c.stime>0"

last_time = (datetime.datetime.now() - timedelta(seconds=3)).strftime('%Y-%m-%d %H:%M:%S.%f')

channels = ', '.join("\'" + ch+"\'" for ch in channel_for_topic_dict.keys())
def get_clicks_5m():
    global last_time
    print datetime.datetime.now()
    conn, cursor = get_postgredb()
    #cursor.execute(click_sql.format(channels, period))
    print cursor.mogrify(click_sql.format(channels, last_time))
    cursor.execute(click_sql.format(channels, last_time))
    rows = cursor.fetchall()
    for r in rows:
        last_time = r[2].strftime('%Y-%m-%d %H:%M:%S.%f')
        print '    {}'.format(last_time)
        ctime_str = r[2].strftime('%Y-%m-%d %H:%M:%S')
        logger_9989.info('    pruduce {}--{}--{}'.format(r[0], r[1], ctime_str))
        nid_queue.produce_user_click_lda(r[0], r[1], ctime_str)
    cursor.close()
    conn.close()


