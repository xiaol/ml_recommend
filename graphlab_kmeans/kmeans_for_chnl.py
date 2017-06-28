# -*- coding: utf-8 -*-
# @Time    : 17/6/28 上午11:25
# @Author  : liulei
# @Brief   : 针对特定频道处理数据
# @File    : kmeans_for_health.py
# @Software: PyCharm Community Edition
import os
from util.logger import Logger
from kmeans import load_newest_models, kmeans_predict
from util.doc_process import get_postgredb_query

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_chnl = Logger('kmeans_chnl', os.path.join(real_dir_path, 'log/kmeans_chnl.log'))
latest_model = load_newest_models(logger_chnl)


def predict_chnl_news(chnl_name, num_limit=None):
    '''
    预测特定频道的新闻
    :param chnl_name: 频道名称
    :param num_limit: 数量限制
    :return:
    '''
    logger_chnl.info('begin to predict {}'.format(chnl_name))
    conn, cursor = get_postgredb_query()
    if num_limit:
        chnl_sql = '''select nid from info_news a inner join channellist_v2 cl
                  on a.chid=cl.id where cl.cname={} limit {}'''
        logger_chnl.info(cursor.mogrify(chnl_sql.format(chnl_name)))
        cursor.execute(chnl_sql.format(chnl_name, num_limit))
    else:
        chnl_sql = '''select nid from info_news a inner join channellist_v2 cl
                  on a.chid=cl.id where cl.cname={}'''
        logger_chnl.info(cursor.mogrify(chnl_sql.format(chnl_name)))
        cursor.execute(chnl_sql.format(chnl_name))

    rows = cursor.fetchall()
    nids = [r[0] for r in rows]
    l = len(nids)
    logger_chnl.info('len of nids is {}'.format(l))
    #分段预测
    if l < 1000:
        kmeans_predict(nids, logger_chnl)
    else:
        n = 0
        while (n + 1000) < len(nids):
            kmeans_predict(nids[n:n + 1000], logger_chnl)
            n += 1000
            logger_chnl.info('{} of {} finished!'.format(n, l))
        kmeans_predict(nids[n - 1000:len(nids)], logger_chnl)

    cursor.close()
    conn.close()

