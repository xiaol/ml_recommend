# -*- coding: utf-8 -*-
# @Time    : 16/12/23 上午10:37
# @Author  : liulei
# @Brief   : 定时任务
# @File    : timed_task.py
# @Software: PyCharm Community Edition

from util.doc_process import get_postgredb
from redis_process import nid_queue
from graphlab_kmeans.kmeans_for_update import chnl_k_dict
import datetime
from datetime import timedelta
import os
from util import logger
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_9981 = logger.Logger('process9981',  os.path.join(real_dir_path,  'log/log_9981.txt'))


#定义取用户点击的循环周期
period = 3
click_sql = "select c.uid, c.nid, c.ctime, c.itime from newsrecommendclick c \
inner join newslist_v2 nl  on c.nid=nl.nid \
INNER JOIN channellist_v2 cl on nl.chid = cl.id \
where cname in ({0}) and c.itime > '{1}' order by c.itime "
#where cname in ({0}) and c.ctime > now() - INTERVAL '{1} second' "

#last_time = (datetime.datetime.now() - timedelta(seconds=3)).strftime('%Y-%m-%d %H:%M:%S.%f')
last_time = datetime.datetime.now() - timedelta(seconds=3)

channels = ', '.join("\'" + ch+"\'" for ch in chnl_k_dict.keys())
def get_clicks_5m():
    logger_9981.info('news epoch...')
    global last_time
    now = datetime.datetime.now()
    if last_time > now:
        logger_9981.info('    **** time error! {}'.format(last_time))
        last_time = now - timedelta(seconds=3)

    conn, cursor = get_postgredb()
    #cursor.execute(click_sql.format(channels, period))
    logger_9981.info("    ctime > '{}'".format(last_time.strftime('%Y-%m-%d %H:%M:%S.%f')))
    cursor.execute(click_sql.format(channels, last_time.strftime('%Y-%m-%d %H:%M:%S.%f')))
    rows = cursor.fetchall()
    for r in rows:
        if r[3] > now:
            continue
        last_time = r[3]    #last_time会保留最晚的时间
        ctime_str = r[2].strftime('%Y-%m-%d %H:%M:%S')
        logger_9981.info('    pruduce {}--{}--{}'.format(r[0], r[1], ctime_str))
        nid_queue.produce_user_click_kmeans(r[0], r[1], ctime_str)
    cursor.close()
    conn.close()


