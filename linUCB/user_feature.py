# -*- coding: utf-8 -*-
# @Time    : 17/6/1 下午12:03
# @Author  : liulei
# @Brief   : 获取用户特征
# @File    : user_feature.py
# @Software: PyCharm Community Edition
'''
1. 取哪些用户?
    最近3个月有点击行为的用户;
    这些用户的点击、不点击用来做样本; 其中不点击行为的数量取点击数
    需要将一天的活跃时间定义为12段,从用户点击提取活跃时间的特征;
'''

import datetime
from util.doc_process import get_postgredb_query
import pandas as pd

def get_active_user_info(min_interval=1, min_click=1):
    '''
    获取活跃用户信息
    :param min_interval: 多少天内有点击行为的才是活跃用户
    :param min_click: 用户最少点击量
    :return:
    '''
    nt = datetime.datetime.now()
    t = nt.strftime('%Y-%m-%d %H:%M:%S')
    #获取活跃用户
    user_sql = "select uid from newsrecommendclick " \
               "where ctime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{} day' " \
               "group by uid HAVING \"count\"(*)>={}"
    conn, cursor = get_postgredb_query()
    cursor.execute(user_sql.format(t, min_interval, min_click))
    rows = cursor.fetchall()
    active_users = [r[0] for r in rows]

    #先获取用户特征
    user_device_sql = "select uid, brand,device_size,network,ctype,province,city,area " \
                      "from user_device " \
                      "where uid in ({})"
    user_raw_info = dict()
    cursor.execute(user_device_sql.format(','.join(str(u) for u in active_users)))
    user_raw = cursor.fetchall()
    for u in user_raw:
        user_raw_info[u[0]] = [u[1], u[2], u[3], u[4], u[5], u[6], u[7]]

    #获取用户活跃时间段及点击的新闻
    user_time_sql = "select nid, ctime from newsrecommendclick " \
                    "where uid={} and " \
                    "ctime > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') - interval '{} day' "
    for u in active_users:
        hour_dict = dict()
        cursor.execute(user_time_sql.format(u, t, min_interval))
        rows = cursor.fetchall()
        for r in rows:
            h = r[1].hour
            hour_dict[h] = 1 if h not in hour_dict else hour_dict[h]+1
        user_raw_info[u].append(hour_dict[h].keys())

    user_csv = pd.Series(user_raw_info).to_csv('user_feature.csv')



    #取负样本

    cursor.close()
    conn.close()


























