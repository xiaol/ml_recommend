# -*- coding: utf-8 -*-
# @Time    : 17/1/4 上午10:10
# @Author  : liulei
# @Brief   : 使用kmeans 算法,为频道内推荐提供比主题模型更粗颗粒度的数据
# @File    : kmeans.py
# @Software: PyCharm Community Edition

import os
import datetime
import graphlab as gl
from util import doc_process
from multiprocessing import Process
import traceback

#添加日志
from util.logger import Logger
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger = Logger('kmeans', os.path.join(real_dir_path, 'log/kmeans.log'))
logger_click = Logger('kmeans_click', os.path.join(real_dir_path, 'log/kmeans_click.log'))
logger_olddata = Logger('kmeans_olddata', os.path.join(real_dir_path, 'log/kmeans_olddata.log'))

#定义全局变量
data_dir = os.path.join(real_dir_path, 'data')
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
#kmeans_model_save_dir = real_dir_path + '/' + 'models/'
from kmeans_for_update import kmeans_model_save_dir
g_channel_kmeans_model_dict = {}


def get_newest_model_dir():
    models = os.listdir(kmeans_model_save_dir)
    ms = {}
    for m in models:
        ms[m] = m.replace('-', '')
    ms_sort = sorted(ms.items(), key=lambda x:x[1])
    if len(ms_sort) != 0:
        return os.path.join(kmeans_model_save_dir, ms_sort.pop()[0])
    else:
        return kmeans_model_save_dir

model_v = os.path.split(get_newest_model_dir())[1]
def load_models(models_dir):
    global g_channel_kmeans_model_dict, model_v
    import os
    model_v = os.path.split(models_dir)[1]
    if len(g_channel_kmeans_model_dict) != 0:
        g_channel_kmeans_model_dict.clear()
    models_files = os.listdir(models_dir)
    for mf in models_files:
        g_channel_kmeans_model_dict[mf] = gl.load_model(models_dir + '/'+ mf)


def load_newest_models(log=logger):
    model_dir = get_newest_model_dir()
    load_models(model_dir)
    log.info('load model: {}'.format(model_dir))

###############################################################################
#@brief  :预测新数据
#@input  :
###############################################################################
nid_sql = 'select a.title, a.content, c.cname \
from info_news a \
inner join channellist_v2 c on a."chid"=c."id" where a.nid={}'

chname_id_dict = {}
def get_chname_id_dict():
    global chname_id_dict
    chname_id_sql = "select id, cname from channellist_v2"
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(chname_id_sql)
    rows = cursor.fetchall()
    for r in rows:
        chname_id_dict[r[1]] = r[0]
    cursor.close()
    conn.close()


insert_sql = "insert into news_kmeans  (nid, model_v, ch_name, cluster_id, chid, ctime) VALUES ({0}, '{1}', '{2}', {3}, {4}, '{5}')"
def kmeans_predict(nid_list, log=logger):
    global g_channel_kmeans_model_dict, chname_id_dict
    log.info('predict : {}'.format(nid_list))
    if len(g_channel_kmeans_model_dict) == 0:
        load_newest_models(log)
    if (len(chname_id_dict)) == 0:
        get_chname_id_dict()
    nid_info = {}
    for nid in nid_list:
        conn, cursor = doc_process.get_postgredb_query()
        cursor.execute(nid_sql.format(nid))
        row = cursor.fetchone()
        if not row:
            log.info('do not get info of {}'.format(nid))
            continue
        title = row[0]
        content_list = row[1]
        chanl_name = row[2]

        if chanl_name not in g_channel_kmeans_model_dict:
            continue

        txt = ''
        for content in content_list:
            if 'txt' in content.keys():
                txt += content['txt'].encode('utf-8')
        total_txt = title + txt
        word_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
        nid_info[nid] = [chanl_name, ' '.join(word_list)]
        cursor.close()
        conn.close()

    for chname in g_channel_kmeans_model_dict.keys():
        nids = []
        doc_list = []
        for nid in nid_info.keys():
            if nid_info[nid][0] == chname:
                nids.append(nid)
                doc_list.append(nid_info[nid][1])

        print 'news num of ' + chname + ' is ' + str(len(nids))
        log.info('news num of {} is {}'.format(chname, len(nids)))
        if len(nids) == 0:
            continue
        ws = gl.SArray(doc_list)
        docs = gl.SFrame(data={'X1': ws})
        docs = gl.text_analytics.count_words(docs['X1'])
        docs = gl.SFrame(docs)
        pred = g_channel_kmeans_model_dict[chname].predict(docs, output_type = 'cluster_id')
        if len(nids) != len(pred):
            log.info('len(nids) != len(pred)')
            return
        conn, cursor = doc_process.get_postgredb()
        for i in xrange(0, len(pred)):
            #入库
            now = datetime.datetime.now()
            time_str = now.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(insert_sql.format(nids[i], model_v, chname, pred[i], chname_id_dict[chname], time_str))

        conn.commit()
        cursor.close()
        conn.close()


nt_sql = "select ch_name, cluster_id, model_v from news_kmeans where nid = {0}"
ut_sql = "select times from user_kmeans_cluster where uid = {0} and model_v = '{1}' and ch_name = '{2}' and cluster_id ='{3}' "
ut_update_sql = "update user_kmeans_cluster set times='{0}', create_time = '{1}', fail_time='{2}' where " \
                "uid='{3}' and model_v = '{4}' and ch_name = '{5}' and cluster_id='{6}'"
user_topic_insert_sql = "insert into user_kmeans_cluster (uid, model_v, ch_name, cluster_id, times, create_time, fail_time, chid) " \
                        "VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}')"
################################################################################
#@brief : 添加用户点击
################################################################################
from datetime import timedelta
def predict_click(click_info, log=logger_click):
    global chname_id_dict
    if (len(chname_id_dict)) == 0:
        get_chname_id_dict()
    uid = click_info[0]
    nid = click_info[1]
    t = datetime.datetime.now()
    time_str = t.strftime('%Y-%m-%d %H:%M:%S')
    log.info('consume kmenas -----{} {} {}'.format(uid, nid, time_str))
    ctime = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    valid_time = ctime + timedelta(days=15) #有效时间定为30天
    fail_time = valid_time.strftime('%Y-%m-%d %H:%M:%S')
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(nt_sql.format(nid)) #获取nid可能的话题
    rows = cursor.fetchall()
    for r in rows:
        ch_name = r[0]
        cluster_id = r[1]
        local_model_v = r[2]
        conn2, cursor2 = doc_process.get_postgredb()
        cursor2.execute(ut_sql.format(uid, local_model_v, ch_name, cluster_id))
        rows2 = cursor2.fetchone()
        if rows2: #该用户已经关注过该topic_id, 更新probability即可
            times = 1 + rows2[0]
            log.info("    update '{0}' '{1}' '{2}' '{3}' '{4}'".format(uid, nid, model_v, ch_name, cluster_id))
            cursor2.execute(ut_update_sql.format(times, time_str, fail_time, uid, local_model_v, ch_name, cluster_id))
        else:
            log.info("    insert '{0}' '{1}' '{2}' '{3}' '{4}'".format(uid, nid, model_v, ch_name, cluster_id))
            cursor2.execute(user_topic_insert_sql.format(uid, local_model_v, ch_name, cluster_id, '1', time_str, fail_time, chname_id_dict[ch_name]))
        conn2.commit()
        conn2.close()
    cursor.close()
    conn.close()


#使用新模型处理旧新闻和点击
def deal_old_news_clicks(day=10, deal_news=True, deal_click=True):
    try:
        from util import doc_process
        logger_olddata.info('deal_old_news_clicks begin....')
        from redis_process import nid_queue
        conn, cursor = doc_process.get_postgredb_query()
        if deal_news:
            nid_queue.clear_queue_kmeans()
            s_new = "select nid from newslist_v2 where (ctime > now() - interval '{} day')  and chid not in (44) and state=0"
            cursor.execute(s_new.format(day))
            rows = cursor.fetchall()
            nids = []
            for r in rows:
                nids.append(r[0])
            l = len(nids)

            if len(nids) < 1000:
                kmeans_predict(nids, logger_olddata)
            else:
                n = 0
                while (n + 1000) < len(nids):
                    kmeans_predict(nids[n:n + 1000], logger_olddata)
                    n += 1000
                    logger_olddata.info('{} of {} finished!'.format(n, l))
                kmeans_predict(nids[n - 1000:len(nids)], logger_olddata)

        if deal_click:
            nid_queue.clear_kmeans_queue_click()
            logger_olddata.info('    deal_old_news_click--- predict click begin...')
            s_click = "select uid, nid, ctime from newsrecommendclick where ctime > now() - interval '{} day'"
            cursor.execute(s_click.format(day))
            rows = cursor.fetchall()
            clicks = []
            for r in rows:
                ctime_str = r[2].strftime('%Y-%m-%d %H:%M:%S')
                clicks.append((r[0], r[1], ctime_str))

            for click in clicks:
                predict_click(click, logger_olddata)
        conn.close()
        logger_olddata.info('deal_old_news_clicks finished....')
    except:
        conn.close()
        logger_olddata.exception(traceback.format_exc())



