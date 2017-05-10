# -*- coding: utf-8 -*-
# @Time    : 17/3/16 下午5:22
# @Author  : liulei
# @Brief   : 创建model。 进程9987
# @File    : model_create.py
# @Software: PyCharm Community Edition
################################################################################
# 创建模型流程
#           1. 获取新闻
#           2. 训练模型
#           3. 保存模型
################################################################################

import os
import datetime
from datetime import timedelta
import json
import traceback
from util.logger import Logger
from data_process import DocProcess
import graphlab as gl
from data_process import get_news_words
from util.doc_process import get_postgredb
from util.doc_process import get_postgredb_query

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger = None
def set_logger(port):
    logger = Logger('process'+str(port),  os.path.join(real_dir_path,  'log/log_{}.txt'.format(port)))
    logger.info('pid {} begin!'.format(port))

#logger9987 = Logger('process9987',  os.path.join(real_dir_path,  'log/log_9987.txt'))
logger_9988 = Logger('process9988_3',  os.path.join(real_dir_path,  'log/log_9988_3.txt'))
logger_9989 = Logger('process9989_3',  os.path.join(real_dir_path,  'log/log_9989_3.txt'))
logger_9990 = Logger('process9990_3',  os.path.join(real_dir_path,  'log/log_9990_3.txt'))

data_dir = os.path.join(real_dir_path, 'data')
model_base_path = os.path.join('/root/ossfs', 'topic_models')  #模型保存路径
model_base_new_path = os.path.join('/root/ossfs/', 'topic_models/000')  #模型测试路径
model_version = ''  #模型版本
model_instance = None
save_model_sql = "insert into topic_models_v2 (model_v, topic_id, topic_words) VALUES (%s, %s, %s)"
insert_sql = "insert into news_topic_v2 (nid, model_v, topic_id, probability, ctime) values(%s, %s, %s, %s, %s)"

class TopicModel(object):
    '''create topic model class'''
    def __init__(self, data_path=None, model_save_path=None):
        self.data_path = data_path
        self.version = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        if model_save_path:
            self.save_path = os.path.join(model_save_path, self.version)
        self.model = None


    def create(self):
        #logger_9987.info('TopicModel::create begin ...')
        docs_sframe = gl.SFrame.read_csv(self.data_path, header=True)
        docs = gl.text_analytics.count_words(docs_sframe['doc'])
        docs = gl.text_analytics.trim_rare_words(docs, threshold=30, delimiters=None)
        self.model = gl.topic_model.create(docs, num_iterations=1000, num_burnin=100, num_topics=5000)

        sf = self.model.get_topics(num_words=20, output_type='topic_words')
        conn, cursor = get_postgredb()
        for i in xrange(0, len(sf)):
            try:
                keys_words_jsonb = json.dumps(sf[i]['words'])
                cursor.execute(save_model_sql, [self.version, str(i), keys_words_jsonb])
                conn.commit()
            except Exception:
                print 'save model to db error'
        conn.close()
        del docs_sframe
        del docs
        #logger_9987.info('TopicModel::create finished!')


    def create_and_save(self):
        self.create()
        #logger_9987.info('   topic model create finished!')
        self.model.save(self.save_path)
        #logger_9987.info('   topic model save finished!')


def create_topic_model():
    try:
        global model_instance
        #data_path = '/root/workspace/news_api_ml/graphlab_lda/data/2017-03-17-15-02-05/体育'
        data_path = os.path.join(get_newest_dir(data_dir), 'data_after.csv')
        model_instance = TopicModel(data_path, model_base_new_path)
        model_instance.create_and_save()
        print '************** create_topic_model finished************'
        #model_instance.create()
    except:
        traceback.print_exc()
        #logger_9987.exception(traceback.format_exc())


# -------------------------------我是分割线, 下面是预测新闻主题--------------------
def load_topic_model(model_path):
    logger_9988.info('load_topic_model {} begin ...'.format(model_path))
    global model_instance
    if not model_instance:
        model_instance = TopicModel()
        model_instance.version = os.path.split(model_path)[-1]
        model_instance.model = gl.load_model(model_path)
    logger_9988.info('load_topic_model finished!')


#获取一个文件夹下最新版的文件夹
def get_newest_dir(dir):
    #models_dir = real_dir_path + '/models'
    models = os.listdir(dir)
    ms = {}
    for m in models:
        ms[m] = m.replace('-', '')
    ms_sort = sorted(ms.items(), key=lambda x:x[1])
    #return model_dir + ms_sort.pop()[0]
    return os.path.join(dir,  ms_sort.pop()[0])


#处理部分久nid
def deal_old_nids(nid_list):
    try:
        s = 'select nid from news_topic_v2 where nid in {}'
        conn, cursor = get_postgredb_query()
        cursor.execute(s.format(tuple(nid_list)))
        rows = cursor.fetchall()
        nid_set = set(nid_list)
        exist_set = set(rows)
        to_deal_set = nid_set - exist_set
        predict_nids(list(to_deal_set))
        conn.close()
    except:
        conn.close()
        raise


def predict_nids(nid_list, for_new_version=False):
    global model_instance
    if not model_instance:
        #p = '/root/ossfs/topic_models/2017-03-20-19-21-21'
        #load_topic_model(p)
        #'''
        if for_new_version:
            load_topic_model(get_newest_dir(model_base_new_path))
        else:
            load_topic_model(get_newest_dir(model_base_path))
        #'''
    return predict(model_instance, nid_list)

def predict(model, nid_list):
    logger_9988.info('predict {}'.format(nid_list))
    t0 = datetime.datetime.now()
    nid_words_dict = get_news_words(nid_list)
    nids = []
    doc_list = []
    for item in nid_words_dict.items():
        nids.append(item[0])
        doc_list.append(item[1])
    ws = gl.SArray(doc_list)
    docs = gl.SFrame(data={'X1':ws})
    docs = gl.text_analytics.count_words(docs['X1'])
    pred = model.model.predict(docs,
                              output_type='probability',
                              num_burnin=50)
    #pred保存的是每个doc在所有主题上的概率值
    props_list = [] #所有文档的主题-概率对儿
    for doc_index in xrange(len(pred)):  #取每个doc的分布
        doc_props = pred[doc_index]
        index_val_dict = {}
        for k in xrange(len(doc_props)):
            if doc_props[k] > 0.1:
                index_val_dict[k] = doc_props[k] #{ topic1:0.3, topic2:0.2, ...}
        sort_prop = sorted(index_val_dict.items(), key=lambda d: d[1], reverse=True)
        props = [] #本文档的主题-概率对儿 # [(5, 0.3), (3, 0.2), ...]
        for i in xrange(min(3, len(sort_prop))):
            if i == 0:
                props.append(sort_prop[i])
            else:
                if sort_prop[i][1] > 0.5 * sort_prop[i-1][1]: #大于0.1并且与前一个概率差别不到一倍
                    props.append(sort_prop[i])
                else:
                    break

        props_list.append(props)   # [ [(5, 0.3), (3, 0.2)..], ....  ]
    #入库
    insert_list = []
    str_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    res_dict_list = []
    for n in xrange(len(nids)):
        for m in xrange(len(props_list[n])):
            topic_id = props_list[n][m][0]
            prop = props_list[n][m][1]
            insert_list.append((nids[n], model.version, topic_id, prop, str_time))
            '''
            sf = model.model.get_topics(num_words=20,
                                       output_type='topic_words')
            info_dict = {}
            info_dict['nid'] = nids[n]
            info_dict['model_v'] = model_version
            info_dict['topic_id'] = topic_id
            info_dict['probability'] = prop
            info_dict['topic_words'] = sf[topic_id]['words']
            res_dict_list.append(info_dict)
            '''
    conn, cursor = get_postgredb()
    cursor.executemany(insert_sql, insert_list)
    conn.commit()
    conn.close()
    t1 = datetime.datetime.now()
    logger_9988.info('prediction takes {}s'.format((t1 - t0).total_seconds()))
    return res_dict_list


#预测多个点击
def predict_clicks(clicks, model_v=None):
    for click in clicks:
        predict_click(click, model_v)

# -------------------------------我是分割线, 下面是预测用户点击------------------
nt_sql = "select topic_id, probability from news_topic_v2 where nid = {0} and model_v = '{1}'"
ut_sql = "select probability from user_topics_v2 where uid = {0} and model_v = '{1}' and topic_id ='{2}' "
user_topic_insert_sql = "insert into user_topics_v2 (uid, model_v, topic_id, probability, create_time, fail_time) " \
                        "VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')"
ut_update_sql = "update user_topics_v2 set probability='{0}', create_time = '{1}', fail_time='{2}' where " \
                "uid='{3}' and model_v = '{4}' and topic_id='{5}'"
#预测用户点击行为
def predict_click(click_info, model_v = None):
    try:
        if not model_v:
            model_v = os.path.split(get_newest_dir(model_base_path))[-1]
        uid = click_info[0]
        nid = click_info[1]
        if isinstance(click_info[2], basestring):
            time_str = click_info[2]
            ctime = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        else:
            ctime = click_info[2]
            time_str = ctime.strftime('%Y-%m-%d %H:%M:%S')
        logger_9990.info("consume click: uid={}, nid={}, time_str={}".format(uid, nid, time_str))
        valid_time = ctime + timedelta(days=30) #有效时间定为30天
        fail_time = valid_time.strftime('%Y-%m-%d %H:%M:%S')
        conn, cursor = get_postgredb_query()
        cursor.execute(nt_sql.format(nid, model_v)) #获取nid可能的话题
        rows = cursor.fetchall()
        for r in rows:
            topic_id = r[0]
            probability = r[1]
            conn2, cursor2 = get_postgredb()
            cursor2.execute(ut_sql.format(uid, model_v, topic_id))
            rows2 = cursor2.fetchone()
            if rows2: #该用户已经关注过该topic_id, 更新probability即可
                new_prop = probability + rows2[0]
                logger_9990.info('update: uid={}, topic_id={}'.format(uid, topic_id))
                cursor2.execute(ut_update_sql.format(new_prop, time_str, fail_time, uid, model_v, topic_id))
            else:
                cursor2.execute(user_topic_insert_sql.format(uid, model_v, topic_id, probability, time_str, fail_time))
            conn2.commit()
            conn2.close()
        cursor.close()
        conn.close()
    except:
        traceback.print_exc()



