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
kmeans_model_save_dir = real_dir_path + '/' + 'models/'
if not os.path.exists(kmeans_model_save_dir):
    os.mkdir(kmeans_model_save_dir)
g_channel_kmeans_model_dict = {}
#chnl_k_dict = {'体育':20, '娱乐':10, '社会':10, '科技':12, '国际':5}
#chnl_k_dict = {'体育':20}
#chnl_k_dict = {'娱乐':20, '社会':20, '国际':10}
#chnl_k_dict = {'体育':20, '娱乐':20, '社会':20, '科技':20, '国际':10}
chnl_k_dict = {'财经':20, '股票':10, '故事':20, '互联网':20, '健康':30, '军事':20,
               '科学':20, '历史':30, '旅游':20, '美食':20, '美文':20, '萌宠':20,
               '汽车':30, '时尚':30, '探索':30, '外媒':30, '养生':30, '影视':30,
               '游戏':40, '育儿':20,
               '体育':20, '娱乐':10, '社会':10, '科技':12, '国际':5}

chnl_k_dict = {'财经':20, '股票':1, '故事':20, '互联网':20, '健康':30, '军事':20,
               '科学':20, '历史':30, '旅游':20, '美食':20, '美文':20, '萌宠':20,
               '汽车':30, '时尚':30, '探索':10, '外媒':30, '养生':30, '影视':30,
               '游戏':30, '育儿':20,'体育':20, '娱乐':10, '社会':10,'科技':12,
               '国际':5, '美女': 1, '搞笑': 1, '趣图':1, '风水玄学':10, '本地':10,
               '自媒体':20, '奇闻':20}


#chnl_newsnum_dict = {'财经':20000, '股票':10000, '故事':10000, '互联网':20000, '健康':20000, '军事':10000,
#                     '科学':10000, '历史':10000, '旅游':10000, '美食':10000, '美文':10000, '萌宠':20000,
#                     '汽车':20000, '时尚':20000, '探索':1500, '外媒':10000, '养生':20000, '影视':20000,
#                     '游戏':20000, '育儿':20000, '体育':20000, '娱乐':20000, '社会':30000, '科技':20000,
#                     '国际':20000,'美女': 100, '搞笑': 100, '趣图':100, '风水玄学':10000, '本地':20000,
#                     '自媒体':10000, '奇闻':10000}
chnl_newsnum_dict = {'财经':50, '股票':50}

#创建新版本模型子进程
def create_kmeans_core(chname, docs, model_save_dir):
    try:
        global g_channel_kmeans_model_dict
        #logger.info('---begin to deal with {}'.format(chname))
        print 'begin to create kmeans model for {}'.format(chname)
        trim_sa = gl.text_analytics.trim_rare_words(docs, threshold=5, to_lower=False)
        docs_trim = gl.text_analytics.count_words(trim_sa)
        model = gl.kmeans.create(gl.SFrame(docs_trim),
                                 num_clusters=chnl_k_dict[chname],
                                 max_iterations=200)
        g_channel_kmeans_model_dict[chname] = model
        #save model to file
        model.save(model_save_dir+'/'+chname)
        print 'create kmeans model for {} finish'.format(chname)
    except:
        traceback.print_exc()


#创建新版本的模型
def create_new_kmeans_model():
    try:
        t0 = datetime.datetime.now()
        global kmeans_model_save_dir,  model_v
        model_create_time = datetime.datetime.now()
        time_str = model_create_time.strftime('%Y-%m-%d-%H-%M-%S')
        data_dir_v = os.path.join(data_dir, time_str)
        model_v = kmeans_model_save_dir + time_str
        if not os.path.exists(model_v):
            os.mkdir(model_v)
            logger.info('create kmeans models {}'.format(time_str))
        if not os.path.exists(data_dir_v):
            os.mkdir(data_dir_v)

        from util.doc_process import coll_cut_extract
        coll_cut_extract(chnl_newsnum_dict, data_dir_v, os.path.join(data_dir_v, 'idf.txt'))
        news = gl.SFrame.read_csv(os.path.join(data_dir_v, 'cut_extract.csv'))
        chnls = news['chnl']  #SArray类型
        nids = news['nid']
        docs = news['doc']
        chnl_doc_dict = dict()
        #提取各个频道的新闻
        for i in xrange(chnls.size()):
            if chnls[i] not in chnl_doc_dict:
                print chnls[i]
                chnl_doc_dict[chnls[i]] = []
            chnl_doc_dict[chnls[i]].append(docs[i])
        #单进程训练
        for item in chnl_doc_dict.items():
            print type(item[1])
            create_kmeans_core(item[0], gl.SArray(item[1]), model_v)
        t1 = datetime.datetime.now()
        time_cost = (t1 - t0).seconds
        print 'create models finished!! it cost ' + str(time_cost) + '\'s'
    except:
        traceback.print_exc()



def get_newest_model_dir():
    global kmeans_model_save_dir
    models = os.listdir(kmeans_model_save_dir)
    ms = {}
    for m in models:
        ms[m] = m.replace('-', '')
    ms_sort = sorted(ms.items(), key=lambda x:x[1])
    if len(ms_sort) != 0:
        return kmeans_model_save_dir + ms_sort.pop()[0]
    else:
        return kmeans_model_save_dir

#初始化模型版本号为最新的保存的模型
model_v = os.path.split(get_newest_model_dir())[1]
def create_model_proc(chname, model_save_dir=None):
    if chname not in chnl_k_dict.keys():
        return
    global g_channle_kmeans_model_dict, data_dir
    print '******************{}'.format(chname)
    docs = gl.SFrame.read_csv(os.path.join(data_dir, chname), header=False)
    trim_sa = gl.text_analytics.trim_rare_words(docs['X1'], threshold=5, to_lower=False)
    docs = gl.text_analytics.count_words(trim_sa)
    model = gl.kmeans.create(gl.SFrame(docs), num_clusters=chnl_k_dict[chname],
                             max_iterations=200)
    print 'create kmeans model for {} finish'.format(chname)
    g_channel_kmeans_model_dict[chname] = model
    #save model to file
    if model_save_dir:
        model.save(model_save_dir+'/'+chname)

def create_kmeans_models():
    global kmeans_model_save_dir, g_channle_kmeans_model_dict, model_v
    model_create_time = datetime.datetime.now()
    time_str = model_create_time.strftime('%Y-%m-%d-%H-%M-%S')
    model_v = kmeans_model_save_dir + time_str
    if not os.path.exists(model_v):
        os.mkdir(model_v)
        logger.info('create kmeans models {}'.format(time_str))

    t0 = datetime.datetime.now()
    for chanl in chnl_k_dict.keys():
        create_model_proc(chanl, model_v) #只能单进程。 gl的数据结构(SFrame等)无法通过传递给子进程
    t1 = datetime.datetime.now()
    time_cost = (t1 - t0).seconds
    print 'create models finished!! it cost ' + str(time_cost) + '\'s'


def load_models(models_dir):
    print 'load_models()'
    global g_channel_kmeans_model_dict, model_v
    import os
    model_v = os.path.split(models_dir)[1]
    if len(g_channel_kmeans_model_dict) != 0:
        g_channel_kmeans_model_dict.clear()
    models_files = os.listdir(models_dir)
    for mf in models_files:
        print '    load ' + mf
        print models_dir
        g_channel_kmeans_model_dict[mf] = gl.load_model(models_dir + '/'+ mf)


def load_newest_models():
    load_models(get_newest_model_dir())

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
        load_newest_models()
    if (len(chname_id_dict)) == 0:
        get_chname_id_dict()
    nid_info = {}
    for nid in nid_list:
        conn, cursor = doc_process.get_postgredb_query()
        cursor.execute(nid_sql, [nid])
        row = cursor.fetchone()
        if not row:
            print 'Error: do not get info of nid: ' + str(nid)
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

    #clstid_nid_dict = {}
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

            #if pred[i] not in clstid_nid_dict.keys():
            #    clstid_nid_dict[pred[i]] = []
            #    clstid_nid_dict[pred[i]].append(nids[i])
            #else:
            #    clstid_nid_dict[pred[i]].append(nids[i])
        conn.commit()
        cursor.close()
        conn.close()
    #print clstid_nid_dict
    #return clstid_nid_dict


#nt_sql = "select ch_name, cluster_id from news_kmeans where nid = {0} and model_v = '{1}' "
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
    time_str = click_info[2]
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


s1 = 'select * from user_kmeans_cluster'
def updateModel():
    conn, cursor = doc_process.get_postgredb()
    cursor.execute(s1) #获取nid可能的话题
    rows = cursor.fetchall()
    print len(rows)
    cursor.close()
    conn.close()

s2 ="update user_kmeans_cluster set model_v='{0}'"
def updateModel2():
    conn, cursor = doc_process.get_postgredb()
    cursor.execute(s2.format('2017-01-17-17-27-04')) #获取nid可能的话题
    conn.commit()
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
            #s_new = "select nid from newslist_v2 where (ctime > now() - interval '{} day') and chid not in (44) and state=0"
            s_new = "select nid from newslist_v2 where (ctime > now() - interval '10 day') and (ctime < now() - interval '3 day')  and chid not in (44) and state=0"
            #cursor.execute(s_new.format(day))
            cursor.execute(s_new)
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
            #s_click = "select uid, nid, ctime from newsrecommendclick where (ctime > now() - interval '{} day') "
            s_click = "select uid, nid, ctime from newsrecommendclick where (ctime > now() - interval '10 day') and (ctime < now() - interval '3 day') "
            #cursor.execute(s_click.format(day))
            cursor.execute(s_click)
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



