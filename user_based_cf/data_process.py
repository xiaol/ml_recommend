# -*- coding: utf-8 -*-
# @Time    : 17/4/5 下午1:56
# @Author  : liulei
# @Brief   : 协同过滤数据处理; user-based cf算法。 首先收集用户点击行为; 用户相似度矩阵每隔一两天就需要更新
#             item使用LDA形成的topic_id代替news_id,降维
#             新表: user_topic_cf  ----- 最终的结果, 用户的邻居推荐的topic
#                   user_similarity_cf ------- 用户相似性的表
# @File    : data_process.py
# @Software: PyCharm Community Edition
import os
import traceback
from util import logger
import pandas as pd
from util.doc_process import get_postgredb_query
from util.doc_process import get_postgredb
import datetime
import math
import json


real_dir_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
log_cf = logger.Logger('log_cf', os.path.join(real_dir_path, 'log', 'log.txt'))


################################################################################
#@brief: 获取最新的topic版本
################################################################################
def get_newest_topic_v():
    topic_sql = "select model_v from user_topics_v2 group by model_v"
    conn, cursor = get_postgredb_query()
    cursor.execute(topic_sql)
    rows = cursor.fetchall()
    topic_vs = []
    for row in rows:
        topic_vs.append(row[0])
    conn.close()
    return max(topic_vs)


click_file = ''
click_query_sql = "select uid, nid, ctime from newsrecommendclick where ctime > now() - interval '%s day'"
#收集用户一段时间内的的点击行为
def coll_click():
    global click_file
    try:
        conn, cursor = get_postgredb_query()
        cursor.execute(click_query_sql, (30,))
        rows = cursor.fetchall()
        user_ids = []
        nid_ids = []
        click_time = []
        for r in rows:
            user_ids.append(r[0])
            nid_ids.append(r[1])
            click_time.append(r[2])

        df = pd.DataFrame({'uid':user_ids, 'nid':nid_ids, 'ctime':click_time}, columns=('uid', 'nid', 'ctime'))
        time_str = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        click_file = os.path.join(real_dir_path, 'data', time_str)
        df.to_csv(click_file, index=False)
        #calculate similarity.  user_dict is a map of userid and index, W is simility matrix with index representing userid
        W = get_user_click_similarity(user_ids, nid_ids, click_time)
        conn.close()
    except:
        log_cf.exception(traceback.format_exc())
        conn.close()


#uid=0是旧版app,没有确切的uid。所有旧版app的使用者的id都是0
user_topic_prop_sql = "select uid, topic_id, probability from user_topics_v2 where model_v = '{}' and uid != 0 and create_time > now() - interval '20 day'"
def coll_user_topics(model_v, time_str):
    try:
        log_cf.info('coll_user_topics begin ...')
        conn, cursor = get_postgredb_query()
        cursor.execute(user_topic_prop_sql.format(model_v))
        rows = cursor.fetchall()
        user_ids = []
        topic_ids = []
        props = []
        log_cf.info('query user topic finished. {} item found.'.format(len(rows)))
        user_topic_prop_dict = {}
        for r in rows:
            user_ids.append(r[0])
            topic_ids.append(r[1])
            props.append(r[2])
            if r[0] not in user_topic_prop_dict:
                user_topic_prop_dict[r[0]] = dict()
            user_topic_prop_dict[r[0]][r[1]] = r[2]

        df = pd.DataFrame({'uid': user_ids, 'topic': topic_ids, 'property': props}, columns=('uid', 'topic', 'property'))
        #time_str = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        topic_file = os.path.join(real_dir_path, 'data', 'user_topic_data_'+time_str + '.txt')
        df.to_csv(topic_file, index=False)
        log_cf.info('uid-topic-property are save to {}'.format(topic_file))
        return user_topic_prop_dict, user_ids, topic_ids, props
    except:
        traceback.print_exc()
        log_cf.exception(traceback.format_exc())


def cal_neignbours(user_ids, topic_ids, props, time):
    try:
        #calcute similarity and save
        conn, cursor = get_postgredb()
        W = get_user_topic_similarity(user_ids, topic_ids, props)
        user_neighbour_dict = dict()
        insert_similarity_sql = "insert into user_similarity_cf (uid, similarity, ctime) VALUES ({}, '{}', '{}')"
        for it in W.items():  #save every user's
            master = it[0]
            sims_dict = it[1]
            sims_list = sorted(sims_dict.items(), key= lambda d:d[1], reverse=True)
            topK_list = sims_list[:50]
            user_neighbour_dict[master] = topK_list
            #print cursor.mogrify(insert_similarity_sql.format(master, json.dumps(topK_list), time))
            cursor.execute(insert_similarity_sql.format(master, json.dumps(topK_list), time))
        #'''
        user_user_file = os.path.join(real_dir_path, 'data', 'user_topic_similarity_'+time + '.txt')
        master_user = []
        slave_user = []
        similarity = []
        for item in W.items():
            #master_user.append(item[0])
            for i2 in item[1].items():
                master_user.append(item[0])
                slave_user.append(i2[0])
                similarity.append(i2[1])
        df2 = pd.DataFrame({'uid1':master_user, 'uid2':slave_user, 'similarity':similarity}, columns=('uid1', 'uid2', 'similarity'))
        df2.to_csv(user_user_file, index=False)
        log_cf.info('uid1-uid2-similarity are save to {}'.format(user_user_file))
        #'''
        conn.commit()
        conn.close()
        print 'finished!!'
        return user_neighbour_dict
    except:
        traceback.print_exc()
        log_cf.exception(traceback.format_exc())



#获取item-user倒排表, 格式  {nid1:[uid1, uid2...], nid2:...}
def get_user_click_similarity(users, nids, times):
    user_set = set(users)
    #记录用户id的索引id
    user_dict = {}
    user_invert_dict = {}
    n = 0
    for i in user_set:
        user_dict[i] = n
        user_invert_dict[n] = i
        n += 1
    item_user_dict = dict()
    for i in xrange(len(nids)):
        if nids[i] not in item_user_dict.keys():
            item_user_dict[nids[i]] = set()
        item_user_dict[nids[i]].add(user_dict[users[i]])

    C = dict()  #记录用户相关矩阵
    N = dict()  #记录每个用户看了多少item
    for i, i_users in item_user_dict.items():
        for u in i_users:
            N[u] += 1
            for v in i_users:
                if u == v:
                    continue
                C[u][v] += 1

    #calcute final similirity matrix W
    W = dict()
    for u, related_users in C.items():
        for v, cuv in related_users.items():
            W[user_invert_dict[u]][user_invert_dict[v]] = cuv / math.sqrt(N[u] * N[v])

    return W


#获取topic-user倒排表, 格式  {topic1:[uid1, uid2...], topic2:...}
def get_user_topic_similarity(users, topics, props):
    log_cf.info('begin to get_user_topic_similarity...')
    user_set = set(users)
    #记录用户id的索引id
    user_dict = {}
    user_invert_dict = {}
    n = 0
    for i in user_set:
        user_dict[i] = n
        user_invert_dict[n] = i
        n += 1
    #get invert-tabel
    topic_user_dict = dict()
    for i in xrange(len(topics)):
        if topics[i] not in topic_user_dict.keys():
            topic_user_dict[topics[i]] = dict()
        topic_user_dict[topics[i]][user_dict[users[i]]] = props[i]

    #get user relationship matrix
    C = dict()  #记录用户相关矩阵
    N = dict()  #记录每个用户兴趣总值, 所有probability相加
    for i, i_users_prop in topic_user_dict.items():
        for u_prop in i_users_prop.items():  #字典
            if u_prop[0] not in N:
                N[u_prop[0]] = 0
            N[u_prop[0]] += u_prop[1]
            for v_prop in i_users_prop.items():
                if u_prop[0] == v_prop[0]:
                    continue
                if u_prop[0] not in C:
                    C[u_prop[0]] = {}
                if v_prop[0] not in C[u_prop[0]]:
                    C[u_prop[0]][v_prop[0]] = 0
                C[u_prop[0]][v_prop[0]] += min(u_prop[1], v_prop[1])  #取最小的probability作为相似值

    #calcute final similirity matrix W based on user matrix
    W = dict()
    for u, related_users in C.items():
        for v, cuv in related_users.items():
            if (user_invert_dict[v] in W) and (user_invert_dict[u] in W[user_invert_dict[v]]):
                continue
            if user_invert_dict[u] not in W:
                W[user_invert_dict[u]] = dict()
            if user_invert_dict[v] not in W[user_invert_dict[u]]:
                W[user_invert_dict[u]][user_invert_dict[v]] = 0
            #print 'cuv = {} and sqrt = {},  {}'.format(cuv, N[u], N[v])
            #sim = cuv / math.sqrt(N[u] * N[v])
            #if sim != 1 and sim > 0.05:
            W[user_invert_dict[u]][user_invert_dict[v]] = cuv / math.sqrt(N[u] * N[v])

    log_cf.info('finished get_user_topic_similarity...')
    return W


#计算由cf推荐的topic, 这些topic是用户没有点击过的
#相似度作为与邻居的权重; 推荐概率为sum(权重 * topic概率)/邻居数
#@input: user_topic_prop_dict ----{u1: {t1:0.1, t2:0.3, t4:0.1.. }, ...}
#        user_neighbours ----{u1:[(u2, 0.2), (u4, 0.05), ...], ...}
def get_potential_topic(user_topic_prop_dict, user_neighbours, model_v, time):
    log_cf.info('begin to get_potential_topic...')
    potential_utp_dict = dict() #存储每个邻居推荐的topic及对应的概率
    for it in user_neighbours.items():
        u = it[0]
        potential_utp_dict[u] = dict()
        for nei_sim in it[1]: #每个邻居
            nei = nei_sim[0]
            sim = nei_sim[1]
            if sim == 1.0:  #完全相同的用户不需做其他比较
                continue
            nei_topics_prop = user_topic_prop_dict[nei]  #邻居的所有topic
            for tp in nei_topics_prop.items():  #
                if tp[0] not in user_topic_prop_dict[u]: #原用户并没有行为的topic
                    if tp[0] not in potential_utp_dict[u]:
                        potential_utp_dict[u][tp[0]] = sim * tp[1]
                    else:
                        potential_utp_dict[u][tp[0]] += sim * tp[1]

    user_potential_topic_sql = "insert into user_topic_cf (uid, model_v, topic_id, property, ctime) VALUES ({}, '{}', {}, {}, '{}')"
    conn, cursor = get_postgredb()
    for item in potential_utp_dict.items():
        u = item[0]
        for it in item[1].items():
            if it[1] > 0.05:
                cursor.execute(user_potential_topic_sql.format(u, model_v, it[0], it[1], time))
    conn.commit()
    conn.close()
    log_cf.info('finished get_potential_topic...')



################################################################################
#整体流程
################################################################################
def get_user_topic_cf():
    time = datetime.datetime.now()
    time_str = time.strftime('%Y-%m-%d-%H-%M-%S')
    model_v = get_newest_topic_v()
    #读取user-topic-property
    user_topic_prop_dict, user_ids, topic_ids, props = coll_user_topics(model_v, time_str)
    #计算neighbour
    user_neighbours = cal_neignbours(user_ids, topic_ids, props, time_str)
    #计算neighbour推荐的topic
    get_potential_topic(user_topic_prop_dict, user_neighbours, model_v, time)
    print '~~~~~~~~~~~~~~~ all finished~~~~~~~~~~~~~~~~~~~~~'


if __name__ == '__main__':
    #coll_click()
    coll_user_topics()





