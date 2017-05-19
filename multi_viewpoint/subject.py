# -*- coding: utf-8 -*-
# @Time    : 17/4/27 下午5:36
# @Author  : liulei
# @Brief   : 生成专题
# @File    : subject.py
# @Software: PyCharm Community Edition
import requests
import json
from util.doc_process import get_postgredb, get_postgredb_query
from util.logger import Logger
import os
import traceback
from util.doc_process import cut_pos_ltp
import operator
import datetime

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_sub = Logger('subject', os.path.join(real_dir_path,  'log/log_subject.txt'))
prefix = 'http://fez.deeporiginalx.com:9001'
cookie = {'Authorization':'f76f3276c1ac832b935163c451f62a2abf5b253c'}
subject_cover='http://pro-pic.deeporiginalx.com/dcc37b1de772f38776e3a8d945b410ed057832c7e1046859dbe030a642155811.jpg'


#创建专题, 获得专题id
def create_subject(nids):
    try:
        logger_sub.info('create subject for {}'.format(nids))
        create_url = prefix + '/topics'
        conn, cursor = get_postgredb()
        sql = "select title from newslist_v2 where nid in ({})"
        nid_str = ', '.join(str(i) for i in nids)
        cursor.execute(sql.format(nid_str))
        rows = cursor.fetchall()
        sub_name = choose_subject_name([r[0] for r in rows])
        conn.close()

        data = {'name': sub_name, 'type': 1, 'cover': subject_cover}
        logger_sub.info('create subject {}'.format(sub_name))
        response = requests.post(create_url, data=data, cookies=cookie)
        content = json.loads(response.content)
        if 'id' not in content:
            logger_sub.info('error to create subject : {}'.format(content))
            return
        return content['id']
    except:
        logger_sub.exception(traceback.format_exc())
        return


#创建class,返回class_id
def create_subject_class(sub_id):
    topic_class_url = prefix + '/topic_classes'
    time = datetime.datetime.now()
    #class_name = str(time.month) + '.' + str(time.day) + '.' + str(time.hour) + '.' + str(time.minute)
    class_name = str(time.month) + '.' + str(time.day)
    #检测是否已经存在
    #check_class_ex = "select id, name, order from topicclasslist where topic=%s and name=%s"
    check_class_ex = "select id, name, \"order\" from topicclasslist where topic=%s"
    conn, cursor = get_postgredb_query()
    cursor.execute(check_class_ex, (sub_id, ))
    rows = cursor.fetchall()
    conn.close()
    new_order = -1
    for row in rows:
        if row[1] == class_name:
            return row[0]
        new_order = max(new_order, row[2])

    data = {'topic': sub_id, 'name': class_name, 'order': new_order + 1}
    try:
        response = requests.post(topic_class_url, data=data, cookies=cookie)
        return json.loads(response.content)['id']
    except:
        logger_sub.exception(response.content)
        raise


#专题添加新闻, 并记录专题-topic
def add_news_to_subject(sub_id, class_id, nids):
    add_nid_url = prefix + '/topic_news'
    conn, cursor = get_postgredb()
    sub_nids_sql = "select news from topicnews where topic=%s"
    cursor.execute(sub_nids_sql, (sub_id, ))
    rows = cursor.fetchall()
    old_sub_nids_set = set()
    for r in rows:
        old_sub_nids_set.add(r[0])
    sub_nids_set = set(nids)
    #专题插入新闻
    logger_sub.info('add {} to {}'.format(sub_nids_set - old_sub_nids_set, sub_id))
    added_nids = sub_nids_set - old_sub_nids_set
    for nid in added_nids:
        data = {'topic_id':sub_id, 'news_id':nid, 'topic_class_id':class_id}
        requests.post(add_nid_url, data=data, cookies=cookie)

    #查询专题-topic
    sub_topic_sql = "select model_v, topic_id, probability from subject_topic where subject_id=%s"
    cursor.execute(sub_topic_sql, (sub_id, ))
    sub_topic_dict = dict()
    topic_model_v = ''
    rows = cursor.fetchall()
    for r in rows:
        topic_model_v = r[0]
        sub_topic_dict[r[1]] = r[2]
    old_topics = sub_topic_dict.keys()
    #计算新闻topic
    news_topic_sql = "select topic_id, probability, model_v from news_topic_v2 where nid=%s"
    topic_model_set = set()
    news_topics_dict = dict()
    for nid in added_nids:
        cursor.execute(news_topic_sql, (nid, ))
        rows2 = cursor.fetchall()
        for r in rows2:
            topic_model_set.add(r[2])
            if r[0] in news_topics_dict:
                news_topics_dict[r[0]] += r[1]
            else:
                news_topics_dict[r[0]] = r[1]
    if len(topic_model_set) == 0 or len(topic_model_set) != 1 or \
       (topic_model_v != '' and topic_model_v != list(topic_model_set)[0]):  #包含多个版本的topic信息
        conn.close()
        return
    if topic_model_v == '':
        topic_model_v = list(topic_model_set)[0]
    #更新专题
    for item in news_topics_dict.items():
        if item[0] in sub_topic_dict:
            sub_topic_dict[item[0]] += item[1]/len(added_nids)
        else:
            sub_topic_dict[item[0]] = item[1]/len(added_nids)
    sub_topic_sort = sorted(sub_topic_dict.items(), key=lambda d:d[1], reverse=True)
    time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    update_sub_topic = "update subject_topic set probability=%s, ctime=%s where subject_id=%s and model_v=%s and topic_id=%s"
    insert_sub_topic = "insert into subject_topic (subject_id, model_v, topic_id, probability, ctime) values (%s, %s, %s, %s, %s)"
    for i in range(0, min(len(sub_topic_sort), 10)):
        tid = sub_topic_sort[i][0]
        tp = sub_topic_sort[i][1]
        if tid in old_topics:
            cursor.execute(update_sub_topic, (tp, time, sub_id, topic_model_v, tid))
        else:
            cursor.execute(insert_sub_topic, (sub_id, topic_model_v, tid, tp, time))

    #专题的新闻总数大于5就自动上线
    all_nids = old_sub_nids_set | sub_nids_set
    logger_sub.info('{} -- {}. all nids len is {}'.format(old_sub_nids_set, sub_nids_set, len(all_nids)))
    if len(all_nids) >= 5:
        online_url = prefix + '/topics/online'
        data = {'zt_id': sub_id, 'online': 0}
        requests.get(online_url, params=data, cookies=cookie)

    conn.commit()
    cursor.close()
    conn.close()


#专题添加关键句子
def save_subject_sentences(sub_id, sents):
    #记录专题key_sentence
    conn, cursor = get_postgredb()
    sub_sents_sql = "insert into topic_sentences (topic_id, sentences) values (%s, %s)"
    cursor.execute(sub_sents_sql, (sub_id, json.dumps(sents)))
    conn.commit()
    conn.close()


def update_sub_name_on_nids(sub_id, nids):
    conn, cursor = get_postgredb_query()
    sql = "select title from newslist_v2 where nid in ({})"
    nid_str = ', '.join(str(i) for i in nids)
    cursor.execute(sql.format(nid_str))
    rows = cursor.fetchall()
    mod_name = choose_subject_name([r[0] for r in rows])
    modify_url = prefix + '/topics_modify'
    data = {'id': sub_id, 'name': mod_name}
    respond = requests.post(modify_url, data=data, cookies=cookie)
    logger_sub.info('response:  {}'.format(respond.content))
    logger_sub.info('update {} sub name to {}'.format(sub_id, mod_name))
    cursor.close()
    conn.close()


################################################################################
#@brief: 更新旧专题
#@input: old_sub_id --- 旧专题id
#        sub      --- 添加的专题
################################################################################
def update_sub(old_sub_id, sub):
    #先获取old_sub_id的class id
    logger_sub.info('update_sub {}: {}'.format(old_sub_id, sub))
    conn, cursor = get_postgredb()
    #创建新的class_id
    class_id = create_subject_class(old_sub_id)
    add_news_to_subject(old_sub_id, class_id, sub[1])

    #更新专题名称
    update_sub_name_on_nids(old_sub_id, sub[1])

    #topic中添加key_sentence
    sent_sql = "select sentences from topic_sentences where topic_id=%s"
    cursor.execute(sent_sql, (old_sub_id, ))
    row = cursor.fetchone()
    if row:
        old_sents = row[0]
    else:
        old_sents = []
    added_sen = set(sub[0]) - set(old_sents)
    if len(added_sen) > 0:
        old_sents.extend(added_sen)
    update_sql = "update topic_sentences set sentences=%s where topic_id=%s"
    cursor.execute(update_sql, (json.dumps(old_sents), old_sub_id))
    conn.commit()
    conn.close()


################################################################################
#@brief: 生成专题名称
#@input: 新闻的title列表
################################################################################
def choose_subject_name(name_list):
    #首先去除已经存在的专题名
    check_exist_sql = "select id from topiclist where " \
                      "create_time > now() - interval '7 day' and " \
                      "type = 1 and name=%s"
    conn, cursor = get_postgredb_query()
    for name in name_list:
        cursor.execute(check_exist_sql, (name, ))
        if len(cursor.fetchall()) != 0:
            name_list.remove(name)
    conn.close()

    if len(name_list) == 0:
        raise ValueError('all subject names have existed!')

    word_doc_freq = dict()  #词的
    name_ws = []
    name_num = len(name_list)
    for name in name_list:
        ws = set(cut_pos_ltp(name, return_str=False))
        name_ws.append(ws)
        for w in ws:
            if w in word_doc_freq:
                word_doc_freq[w] += 1
            else:
                word_doc_freq[w] = 1
    words_matter = []
    for item in word_doc_freq.items():
        if item[1] > name_num / 2:
            words_matter.append(item[0])
    words_matter_ratio = []
    for name in name_ws:
        name_matter = name & set(words_matter)
        words_matter_ratio.append(len(name_matter) / float(len(name)))
    index, value = max(enumerate(words_matter_ratio), key=operator.itemgetter(1))
    return name_list[index]

################################################################################
#@brief: 生成专题
#@input: 专题, [ [], [] ], 包含key_sentences 列表和nids列表
################################################################################
def generate_subject(sub):
    try:
        sub_sents = sub[0]
        sub_nids = sub[1]
        ##############检查是否需要新建专题还是更新到旧专题###
        conn, cursor = get_postgredb()
        oldsub_nid_dict = dict()  #记录旧topic--与本sub相同的nid
        nid_old_sub_sql = "select tn.topic, tn.news from topicnews tn " \
                          "inner join topiclist tl on tn.topic=tl.id " \
                          "where news in %s and tl.type=1"
        cursor.execute(nid_old_sub_sql, (tuple(sub_nids), ))
        rows = cursor.fetchall()
        for r in rows:
             if r[0] in oldsub_nid_dict:
                 oldsub_nid_dict[r[0]].append(r[1])
             else:
                 oldsub_nid_dict[r[0]] = [r[1], ]
        update = False
        for item in oldsub_nid_dict.items():
            if float(len(item[1])) >= 0.5 * len(sub_nids):  #sub一半以上的nid包含在旧subject内,则把sub合并进旧subject
                update_sub(item[0], sub)
                update = True
        if update:
            conn.close()
            return


        ##############需要新建专题#######################
        '''
        create_url = prefix + '/topics'
        #set subject name as one title of one piece of news
        sql = "select title from newslist_v2 where nid=%s"
        cursor.execute(sql, (sub_nids[0],))
        rows = cursor.fetchall()
        sub_name = choose_subject_name([r[0] for r in rows])

        data = {'name': sub_name, 'type': 1}
        logger_sub.info('create subject {}'.format(sub_name))
        response = requests.post(create_url, data=data, cookies=cookie)
        content = json.loads(response.content)
        if 'id' not in content:
            logger_sub.info('error to create subject : {}'.format(content))
            return
        sub_id = content['id']
        '''
        sub_id = create_subject(sub_nids)
        if not sub_id:
            return

        '''
        topic_class_url = prefix + '/topic_classes'
        time = datetime.datetime.now()
        class_name = str(time.month) + '.' + str(time.day)
        data = {'topic': sub_id, 'name': class_name}
        response = requests.post(topic_class_url, data=data, cookies=cookie)
        class_id = json.loads(response.content)['id']
        '''
        class_id = create_subject_class(sub_id)

        '''
        add_nid_url = prefix + '/topic_news'
        for nid in sub_nids:
            data = {'topic_id':sub_id, 'news_id':nid, 'topic_class_id':class_id}
            requests.post(add_nid_url, data=data, cookies=cookie)
        '''
        add_news_to_subject(sub_id, class_id, sub_nids)

        '''
        #记录专题key_sentence
        sub_sents_sql = "insert into topic_sentences (topic_id, sentences) values (%s, %s)"
        cursor.execute(sub_sents_sql, (sub_id, json.dumps(sub_sents)))
        '''
        save_subject_sentences(sub_id, sub_sents)
        conn.commit()
        conn.close()
    except:
        logger_sub.exception(traceback.format_exc())
