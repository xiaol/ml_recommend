# -*- coding: utf-8 -*-
# @Time    : 16/11/30 下午2:24
# @Author  : liulei
# @Brief   : 
# @File    : topic_modle.py
# @Software: PyCharm Community Edition
# Download data if you haven't already
import json
import requests
import graphlab as gl
import os
import topic_model_doc_process
from util import doc_process
import traceback
from util import logger
from topic_model_doc_process import channel_for_topic

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger9987 = logger.Logger('process_9987',  os.path.join(real_dir_path,  'log/log_9987_old.txt'))
logger9988 = logger.Logger('process_9988',  os.path.join(real_dir_path,  'log/log_9988_old.txt'))
logger9990 = logger.Logger('process_9990', os.path.join(real_dir_path,  'log/log_9990_old.txt'))


g_channel_model_dict = {}
import datetime
data_dir = real_dir_path + '/data/'
#model_dir = real_dir_path + '/models/'
#model_dir = '~/ossfs/topic_models/'
model_dir = os.path.join('/root/ossfs', 'topic_models')
#model_dir = os.path.join(real_dir_path, 'models')

model_v = ''

save_model_sql = "insert into topic_models (model_v, ch_name, topic_id, topic_words) VALUES (%s, %s, %s, %s)"
def save_model_to_db(model, ch_name):
    global model_v
    model_create_time = datetime.datetime.now()
    #model 版本以时间字符串
    model_v = model_create_time.strftime('%Y%m%d%H%M%S')
    sf = model.get_topics(num_words=20, output_type='topic_words')

    conn, cursor = doc_process.get_postgredb()
    for i in xrange(0, len(sf)):
        try:
            keys_words_jsonb = json.dumps(sf[i]['words'])
            cursor.execute(save_model_sql, [model_v, ch_name, str(i), keys_words_jsonb])
            conn.commit()
        except Exception:
            print 'save model to db error'
    conn.close()


def create_model_proc(csv_file, model_save_dir=None):
    if not os.path.exists(data_dir + csv_file):
        return
    docs = gl.SFrame.read_csv(data_dir+csv_file, header=False)
    docs = gl.text_analytics.count_words(docs['X1'])
    docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
    model = gl.topic_model.create(docs, num_iterations=100, num_burnin=100, num_topics=10000)
    g_channel_model_dict[csv_file] = model
    save_model_to_db(model, csv_file)
    #save model
    if model_save_dir:
        #model.save(model_save_dir+'/'+csv_file)
        model.save(os.path.join(model_save_dir, csv_file))


def create_models():
    logger9987.info("begin to create model ......")
    global model_dir
    model_create_time = datetime.datetime.now()
    time_str = model_create_time.strftime('%Y-%m-%d-%H-%M-%S')
    model_path = os.path.join(model_dir, time_str)
    if not os.path.exists(model_path):
        os.mkdir(model_path)

    for chanl in channel_for_topic:
        create_model_proc(chanl, model_path)
    print 'create models finished!!'


def get_newest_model_dir():
    global model_dir
    #models_dir = real_dir_path + '/models'
    models = os.listdir(model_dir)
    ms = {}
    for m in models:
        ms[m] = m.replace('-', '')
    ms_sort = sorted(ms.items(), key=lambda x:x[1])
    #return model_dir + ms_sort.pop()[0]
    return os.path.join(model_dir,  ms_sort.pop()[0])


def load_models(models_dir):
    logger9988.info("load_models ......")
    global g_channel_model_dict, model_v
    import os
    model_v = os.path.split(models_dir)[1]
    if len(g_channel_model_dict) != 0:
        g_channel_model_dict.clear()
    models_files = os.listdir(models_dir)
    for mf in models_files:
        logger9988.info("   load {}".format(models_dir + '/' + mf))
        g_channel_model_dict[mf] = gl.load_model(models_dir + '/'+ mf)
    logger9988.info("load_models finished!")


def load_newest_models():
    load_models(get_newest_model_dir())


nid_sql = 'select a.title, a.content, c.cname \
from (select * from newslist_v2 where nid=%s) a \
inner join channellist_v2 c on a."chid"=c."id"'
def get_words_on_nid(nid, ch_names):
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(nid_sql, [nid])
    row = cursor.fetchone()
    title = row[0]  #str类型
    content_list = row[1]
    chanl_name = row[2]
    if chanl_name not in ch_names:
        return [], ''
    txt = ''
    for content in content_list:
        if 'txt' in content.keys():
            txt += content['txt'].encode('utf-8')
    total_txt = title + txt
    word_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
    return word_list, chanl_name

def lda_predict_core(nid):
    global g_channel_model_dict
    if len(g_channel_model_dict) == 0:
        load_newest_models()

    words_list, chanl_name = get_words_on_nid(nid, topic_model_doc_process.channel_for_topic)
    if chanl_name not in g_channel_model_dict.keys():
        #print 'Error: channel name is not in models' + '---- ' + chanl_name + " " + str(nid)
        return '', []
    s = ''
    for i in words_list:
        s += i + ' '
    ws = gl.SArray([s,])
    docs = gl.SFrame(data={'X1':ws})
    docs = gl.text_analytics.count_words(docs['X1'])
    docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)

    #预测得分最高的topic
    #pred = g_channel_model_dict[chanl_name].predict(docs)
    #print pred
    #print '%s' % str(sf[pred[0]]['words']).decode('string_escape')
    t = datetime.datetime.now()
    pred2 = g_channel_model_dict[chanl_name].predict(docs,
                                                     output_type='probability',
                                                     num_burnin=30)
    t2 = datetime.datetime.now()
    print 'predict time:'
    print (t2 - t).seconds

    return chanl_name, pred2


def lda_predict(nid):
    global g_channel_model_dict
    chanl_name, pred = lda_predict_core(nid)
    sf = g_channel_model_dict[chanl_name].get_topics(num_words=20,
                                                     output_type='topic_words')
    num_dict = {}
    num = 0
    for i in pred[0]:
        num_dict[i] = num
        num += 1
    probility = sorted(num_dict.items(), key=lambda d: d[0], reverse=True)
    i = 0
    res = {}
    while i < 3 and i < len(probility) and probility[i][0] > 0.1:
        res[probility[i][0]] = sf[probility[i][1]]['words']
        i += 1
    return res


news_topic_sql = "insert into news_topic (nid, model_v, ch_name, topic_id, probability, real_cname, ctime) VALUES (%s,  %s, %s, %s, %s, %s, %s)"
def lda_predict_and_save(nid):
    global model_v
    ch_name, pred = lda_predict_core(nid)
    if len(pred) == 0:
        return

    num_dict = {}
    num = 0
    for i in pred[0]:
        num_dict[i] = num
        num += 1
    probility = sorted(num_dict.items(), key=lambda d: d[0], reverse=True)
    i = 0
    to_save = {}
    while i < 3 and i < len(probility) and probility[i][0] > 0.1:
        to_save[probility[i][1]] = probility[i][0]
        i += 1

    conn, cursor = doc_process.get_postgredb()
    for item in to_save.items():
        cursor.execute(news_topic_sql, [nid, model_v, ch_name, item[0], item[1]])
    conn.commit()
    conn.close()


user_topic_sql = 'select * from usertopics where uid = %s and model_v = %s and ch_name=%s and topic_id = %s'
user_topic_insert_sql = "insert into usertopics (uid, model_v, ch_name, topic_id, probability, create_time, fail_time) " \
                        "VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')"
user_topic_update_sql = "update usertopics set probability='{0}', create_time='{1}', fail_time='{2}'" \
                        "where uid='{3}' and model_v='{4}' and ch_name='{5}' and topic_id='{6}'"
#预测用户话题主逻辑
from datetime import timedelta
def predict_user_topic_core(uid, nid, time_str):
    global g_channel_model_dict, model_v
    ch_name, pred = lda_predict_core(nid)  #预测topic分布

    if len(pred) == 0:
        return
    num_dict = {}
    num = 0 #标记topic的index
    for i in pred[0]:  #pred[0]是property值
        if i < 0.15:  #概率<0.1, 不考虑
            num += 1
            continue
        num_dict[i] = num
        num += 1
    probility = sorted(num_dict.items(), key=lambda d: d[0], reverse=True)
    i = 0
    to_save = {}
    while i < 3 and i < len(probility):
        to_save[probility[i][1]] = probility[i][0]
        i += 1
    try:
        ctime = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        valid_time = ctime + timedelta(days=30) #有效时间定为30天
        fail_time = valid_time.strftime('%Y-%m-%d %H:%M:%S')
    except:
        traceback.print_exc()
        return
    for item in to_save.items():
        topic_id = item[0]
        conn, cursor = doc_process.get_postgredb()
        cursor.execute(user_topic_sql, [uid, model_v, ch_name, topic_id])
        rows = cursor.fetchall()
        if len(rows) == 0: #此版本的model下没有记录该用户的点击行为
            probability = item[1]
            cursor.execute(user_topic_insert_sql.format(uid, model_v, ch_name, topic_id, probability, time_str, fail_time))
        else:
            for r in rows:   #取出已经存在的一栏信息
                org_probability = r[4]
            new_probabiliby = org_probability + item[1]
            cursor.execute(user_topic_update_sql.format(new_probabiliby, time_str, fail_time, uid, model_v, ch_name, topic_id))
        conn.commit()
        conn.close()

    t2 = datetime.datetime.now()


#from psycopg2.extras import Json
#收集用户topic
#nids_info: 包含nid号及nid被点击时间
def coll_user_topics(uid, nids_info):
    for nid_info in nids_info:
        predict_user_topic_core(uid, nid_info[0], nid_info[1])


user_click_sql = "select uid, nid, max(ctime) ctime from newsrecommendclick  where CURRENT_DATE - INTEGER '3' <= DATE(ctime) group by uid,nid"
def get_user_topics():
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(user_click_sql)
    rows = cursor.fetchall()
    user_news_dict = {}
    for r in rows:
        uid = r[0]
        if uid in user_news_dict.keys():
            user_news_dict[uid].add((r[1], r[2]))
        else:
            user_news_dict[uid] = set()
            user_news_dict[uid].add((r[1], r[2]))
    for item in user_news_dict.items():
        coll_user_topics(item[0], item[1].strftime('%Y-%m-%d %H:%M:%S'))


#取十万条新闻加入队列做预测,并保存至数据库
channle_sql ='SELECT a.nid FROM newslist_v2 a \
RIGHT OUTER JOIN (select * from channellist_v2 where cname in ({0})) c \
ON \
a.chid=c.id ORDER BY nid DESC LIMIT {1}'
#search_sql = 'select nid from newslist_v2 ordered by nid  DESC limit "%d" '
def produce_news_topic_manual(num):
    conn, cursor = doc_process.get_postgredb()
    channels = ', '.join("\'" + ch+"\'" for ch in topic_model_doc_process.channel_for_topic)
    cursor.execute(channle_sql.format(channels, num))
    rows = cursor.fetchall()
    import redis_lda
    for r in rows:
        redis_lda.produce_nid(r[0])


###############################################################################
### @brief: 处理自媒体和点集频道
### @input: nid list, 其中nid都是自媒体或者点集的频道
### @output: {nid1:(真实频道, 预测频道), nid2...}
###############################################################################
def get_nid_predict_chname(nid_list):
    try:
        from topic_model_doc_process import channel_for_topic_dict
        nid_chanl_dict = {}
        logger9988.info("predict ----自媒体和点集  {}".format(len(nid_list)))
        #print 'predict ---- 自媒体和点集 ' + str(len(nid_list))
        url = "http://127.0.0.1:9993/ml/newsClassifyOnNids"
        data = {}
        data['nids'] = nid_list
        response = requests.post(url, data=data)
        cont = json.loads(response.content)
        if cont['bSuccess']:
            res = cont['result']
            for r in res:
                if str(r['chid']) in channel_for_topic_dict.keys():
                    nid_chanl_dict[str(r['nid'])] = channel_for_topic_dict[str(r['chid'])]
        else:
            logger9988.info("predict 自媒体失败")
            #print 'predict 自媒体失败'

        return nid_chanl_dict
    except :
        logger9988.error(traceback.format_exc())
        #traceback.print_exc()
        return {}


# @imput: nid_list是nid列表,其中nid是字符串
def predict_topic_nids(nid_list):
    global g_channel_model_dict, model_v
    from topic_model_doc_process import channel_for_topic, extra_channel_for_topic

    nid_info = {}
    logger9988.info(str(nid_list))
    #print nid_list
    for nid in nid_list:
        try:
            conn, cursor = doc_process.get_postgredb_query()
        except:
            logger9988.error(traceback.format_exc())
            #traceback.print_exc()
            continue
        cursor.execute(nid_sql, [nid])
        row = cursor.fetchone()
        title = row[0]
        content_list = row[1]
        chanl_name = row[2]

        if (chanl_name not in channel_for_topic) and (chanl_name not in extra_channel_for_topic):
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

    #主要用于处理自媒体和点集
    extra_chanl_nids = [n for n in nid_info.keys() if nid_info[n][0] in extra_channel_for_topic]
    extra_nid_chname_dict = {}
    if len(extra_chanl_nids) != 0:
        extra_nid_chname_dict = get_nid_predict_chname(extra_chanl_nids)

    now = datetime.datetime.now()
    time_str = now.strftime('%Y-%m-%d %H:%M:%S')

    chname_news_dict = {}
    for chname in channel_for_topic:
        nid_pred_dict = {}
        logger9988.info("predict   {}".format(chname))
        #print 'predict  ' + chname
        if chname not in g_channel_model_dict.keys():
            logger9988.info("{}  is not in model.".format(chname))
            #print chname + 'is not in model'
            continue
        chname_news_dict[chname] = []
        for n in nid_info.items():
            id = n[0]
            if n[1][0] == chname:
                chname_news_dict[chname].append(id) #获取该频道的nid列表
        #添加点集和自媒体的新闻
        for n in extra_nid_chname_dict.keys():
            if extra_nid_chname_dict[n] == chname:
                chname_news_dict[chname].append(n) #获取该频道的nid列表

        if len(chname_news_dict[chname]) == 0:
            logger9988.info('    num of {} is 0. '.format(chname))
            #print '    num of ' + chname + 'is 0'
            continue
        doc_list = []
        for n in chname_news_dict[chname]:
            doc_list.append(nid_info[n][1])
        ws = gl.SArray(doc_list)
        docs = gl.SFrame(data={'X1':ws})
        docs = gl.text_analytics.count_words(docs['X1'])
        docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
        t0 = datetime.datetime.now()
        pred = g_channel_model_dict[chname].predict(docs,
                                                        output_type='probability',
                                                        num_burnin=30)
        t1 = datetime.datetime.now()
        logger9988.info("    predict {0} takes {1}".format(len(doc_list), (t1-t0).total_seconds()))
        #print '    predict ' + str(len(doc_list)) + ' takes ' + str((t1 - t0).seconds)
        for m in xrange(0, pred.size()):
            num_dict = {}
            num = 0
            for i in pred[m]:
                if i <= 0.1: #概率<0.1忽略
                    num += 1
                    continue
                num_dict[i] = num
                num += 1
            probility = sorted(num_dict.items(), key=lambda d: d[0], reverse=True)
            to_save = {}
            k = 0
            while k < 3 and k < len(probility):
                to_save[probility[k][1]] = probility[k][0]
                k += 1
            nid_pred_dict[chname_news_dict[chname][m]] = to_save

        try:
            conn, cursor = doc_process.get_postgredb()
        except:
            logger9988.error(traceback.format_exc())
            continue
        for item in nid_pred_dict.items():
            n = item[0]
            extra_chanl = ''
            if n in extra_nid_chname_dict.keys():
                extra_chanl =nid_info[n][0] #点集或者自媒体
            for pred in item[1].items():
                cursor.execute(news_topic_sql, [n, model_v, chname, pred[0], pred[1], extra_chanl, time_str])
        conn.commit()
        cursor.close()
        conn.close()



ch_sql = "select cname from channellist_v2 cv inner join newslist_v2 nv on cv.id=nv.chid where nv.nid={0}"
nt_sql = "select ch_name, topic_id, probability from news_topic where nid = {0} and model_v = '{1}' "
ut_sql = "select probability from usertopics where uid = {0} and model_v = '{1}' and ch_name = '{2}' and topic_id ='{3}' "
ut_update_sql = "update usertopics set probability='{0}', create_time = '{1}', fail_time='{2}' where " \
                "uid='{3}' and model_v = '{4}' and ch_name = '{5}' and topic_id='{6}'"
#预测用户点击行为
def predict_click(click_info):
    try:
        global model_v
        uid = click_info[0]
        nid = click_info[1]
        time_str = click_info[2]
        ctime = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        valid_time = ctime + timedelta(days=30) #有效时间定为30天
        fail_time = valid_time.strftime('%Y-%m-%d %H:%M:%S')
        conn, cursor = doc_process.get_postgredb_query()
        print nid
        print model_v
        cursor.execute(nt_sql.format(nid, model_v)) #获取nid可能的话题
        rows = cursor.fetchall()
        print 'predict topic num = ' + str(len(rows))
        for r in rows:
            ch_name = r[0]
            topic_id = r[1]
            probability = r[2]
            conn2, cursor2 = doc_process.get_postgredb()
            cursor2.execute(ut_sql.format(uid, model_v, ch_name, topic_id))
            rows2 = cursor2.fetchone()
            if rows2: #该用户已经关注过该topic_id, 更新probability即可
                print 'update'
                new_prop = probability + rows2[0]
                cursor2.execute(ut_update_sql.format(new_prop, time_str, fail_time, uid, model_v, ch_name, topic_id))
            else:
                print 'insert'
                cursor2.execute(user_topic_insert_sql.format(uid, model_v, ch_name, topic_id, probability, time_str, fail_time))
            conn2.commit()
            conn2.close()
        cursor.close()
        conn.close()
    except:
        traceback.print_exc()


