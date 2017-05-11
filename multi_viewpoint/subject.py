# -*- coding: utf-8 -*-
# @Time    : 17/4/27 下午5:36
# @Author  : liulei
# @Brief   : 生成专题
# @File    : subject.py
# @Software: PyCharm Community Edition
import requests
import json
from util.doc_process import get_postgredb_query
from util.logger import Logger
import os

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_sub = Logger('subject', os.path.join(real_dir_path,  'log/log_subject.txt'))
prefix = 'http://fez.deeporiginalx.com:9001'
cookie = {'Authorization':'f76f3276c1ac832b935163c451f62a2abf5b253c'}
################################################################################
#@brief: 更新旧专题
#@input: old_sub_id --- 旧专题id
#        sub      --- 添加的专题
################################################################################
def update_sub(old_sub_id, sub):
    #先获取old_sub_id的class id
    logger_sub.info('update_sub : {}'.format(sub))
    sub_class = "select id from topicclasslist where topic=%s"
    conn, cursor = get_postgredb_query()
    cursor.execute(sub_class, (old_sub_id, ))
    rows = cursor.fetchall()
    if len(rows) == 0:
        raise ValueError('do not find class id for {}'.format(old_sub_id))
    class_id = rows[0]
    add_nid_url = prefix + '/topic_news'
    sub_nids_sql = "select news from topicnews where topic=%s"
    cursor.execute(sub_nids_sql, (old_sub_id, ))
    rows = cursor.fetchall()
    old_sub_nids_set = set()
    for r in rows:
        old_sub_nids_set.add(r[0])
    sub_nids_set = set(sub[1])

    #topic中添加nid
    for nid in (sub_nids_set - old_sub_nids_set):
        data = {'topic_id':old_sub_id, 'news_id':nid, 'topic_class_id':class_id}
        requests.post(add_nid_url, data=data, cookies=cookie)
    #topic中添加key_sentence
    sent_sql = "select sentences from topic_sentences where topic_id=%s"
    cursor.execute(sent_sql, (old_sub_id, ))
    row = cursor.fetchone()  #返回的是关键句子的list
    old_sents = row[0]
    added_sen = set(sub[0]) - set(old_sents)
    if len(added_sen) > 0:
        old_sents.extend(added_sen)
    update_sql = "update topic_sentences set sentences=%s where topic_id=%s"
    cursor.execute(update_sql, (json.dumps(old_sents), old_sub_id))
    conn.commit()
    conn.close()



################################################################################
#@brief: 生成专题
#@input: 专题, [ [], [] ], 包含key_sentences 列表和nids列表
################################################################################
def generate_subject(sub):
    sub_sents = sub[0]
    sub_nids = sub[1]
    ##############检查是否需要新建专题还是更新到旧专题###
    conn, cursor = get_postgredb_query()
    oldsub_nid_dict = dict()  #记录旧topic--与本sub相同的nid
    nid_old_sub_sql = "select topic, news from topicnews where news in %s"
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
    create_url = prefix + '/topics'
    #set subject name as one title of one piece of news
    sql = "select title from newslist_v2 where nid=%s"
    cursor.execute(sql, (sub_nids[0],))
    row = cursor.fetchone()
    sub_name = row[0]
    data = {'name': sub_name}
    logger_sub.info('create subject {}'.format(sub_name))
    response = requests.post(create_url, data=data, cookies=cookie)
    content = json.loads(response.content)
    if 'id' not in content:
        print 'error: ******  id not in content'
        print content
        return
    sub_id = content['id']

    topic_class_url = prefix + '/topic_classes'
    data = {'topic': sub_id, 'name': 'random'}
    response = requests.post(topic_class_url, data=data, cookies=cookie)
    class_id = json.loads(response.content)['id']

    add_nid_url = prefix + '/topic_news'
    for nid in sub_nids:
        data = {'topic_id':sub_id, 'news_id':nid, 'topic_class_id':class_id}
        requests.post(add_nid_url, data=data, cookies=cookie)

    #记录专题key_sentence
    sub_sents_sql = "insert into topic_sentences (topic_id, sentences) values (%s, %s)"
    cursor.execute(sub_sents_sql, (sub_id, json.dumps(sub_sents)))
    conn.commit()
    conn.close()
