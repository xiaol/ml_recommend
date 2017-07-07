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
from heapq import nlargest
from operator import itemgetter

TEST_FLAG = False

real_dir_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
log_cf = logger.Logger('log_cf', os.path.join(real_dir_path, 'log', 'log.txt'))
log_cf_clear_data = logger.Logger('log_cf_clear_data', os.path.join(real_dir_path, 'log', 'log_clear_data.txt'))


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


def coll_user_topics(model_v):
    # uid=0是旧版app,没有确切的uid。所有旧版app的使用者的id都是0
    if TEST_FLAG:
        user_topic_prop_sql = '''select uid, topic_id, probability from user_topics_v2 
                             where model_v = '{}' and uid != 0 and 
                             create_time > now() - interval '10 minute' '''
    else:
        user_topic_prop_sql = '''select uid, topic_id, probability from user_topics_v2 
                             where model_v = '{}' and uid != 0 and 
                             create_time > now() - interval '2 day' '''

    try:
        log_cf.info('    coll_user_topics begin ...')
        conn, cursor = get_postgredb_query()
        cursor.execute(user_topic_prop_sql.format(model_v))
        rows = cursor.fetchall()
        user_ids = []
        topic_ids = []
        props = []
        log_cf.info('    query user topic finished. {} item found.'.format(len(rows)))
        user_topic_prop_dict = {}
        for r in rows:
            user_ids.append(r[0])
            topic_ids.append(r[1])
            props.append(r[2])
            if r[0] not in user_topic_prop_dict:
                user_topic_prop_dict[r[0]] = dict()
            user_topic_prop_dict[r[0]][r[1]] = r[2]

        log_cf.info('    coll_user_topics end')
        del rows
        cursor.close()
        conn.close()
        if TEST_FLAG:
            f = os.path.join(real_dir_path, 'data', 'user_topic.csv')
            df = {'user':user_ids, 'topic':topic_ids, 'prop':props}
            pd.DataFrame(df).to_csv(f, columns=('user', 'topic', 'prop'))
        return user_topic_prop_dict, user_ids, topic_ids, props
    except:
        traceback.print_exc()
        log_cf.exception(traceback.format_exc())


def cal_neignbours(user_ids, topic_ids, props):
    try:
        W = get_user_topic_similarity2(user_ids, topic_ids, props)
        user_neighbour_dict = dict()
        for it in W.items():  #save every user's
            master = it[0]
            sims_dict = it[1]
            user_neighbour_dict[master] = nlargest(30, sims_dict.iteritems(), key=itemgetter(1))
        log_cf.info("    cal_neighbour finished!")
        u_list, u2_list, prop = [], [], []
        for item in user_neighbour_dict.items():
            u_list.append(item[0])
            u2_list.append(item[1][0])
            prop.append(item[1][1])
        if TEST_FLAG:
            f = os.path.join(real_dir_path, 'data', 'sorted_sim.csv')
            pd.DataFrame({'u1':u_list, 'u2':u2_list, 'prop':prop}).to_csv(f, columns=('u1', 'u2', 'prop'))
        return user_neighbour_dict
    except:
        traceback.print_exc()
        log_cf.exception(traceback.format_exc())


def get_user_topic_similarity2(users, topics, props):
    log_cf.info('    begin to get_user_topic_similarity...')
    user_set = set(users)
    topic_user_prop = dict()
    u_total = {}  #记录每个用户兴趣总值, 所有probability相加
    for i in xrange(len(topics)):
        topic_user_prop.setdefault(topics[i], dict())[users[i]] = props[i]
        u_total[users[i]] = u_total.setdefault(users[i], 0) + props[i]

    # get user relationship matrix
    sim_pair_dict = {}  #记录用户相关矩阵
    for user in user_set:
        sim_pair_dict[user] = {}
    for i, i_users_prop in topic_user_prop.items():
        for u_prop in i_users_prop.items():  #字典
            u1 = u_prop[0]
            for v_prop in i_users_prop.items():
                u2 = v_prop[0]
                if u1 == v_prop[0]:
                    continue
                sim_pair_dict[u1][u2] = sim_pair_dict[u1].setdefault(u2, 0) + min(u_prop[1], v_prop[1])

    #calcute final similirity matrix W based on user matrix
    for u, sim_prop in sim_pair_dict.items():
        sim_pair_dict[u][sim_prop[0]] /= math.sqrt(u_total[u] * u_total[sim_prop[0]])
    u_list, u2_list, p_list = [], [], []

    for sim in sim_pair_dict.items():
        u_list.append(sim[0])
        u2_list.append(sim[1][0])
        p_list.append(sim[1][1])

    if TEST_FLAG:
        f = os.path.join(real_dir_path, 'data', 'user_sim.csv')
        pd.DataFrame({'u1':u_list, 'u2':u2_list, 'prop':p_list}).to_csv(f, columns=('u1', 'u2', 'prop'))
    log_cf.info('    finished get_user_topic_similarity...')
    return sim_pair_dict


#获取topic-user倒排表, 格式  {topic1:[uid1, uid2...], topic2:...}
def get_user_topic_similarity(users, topics, props):
    log_cf.info('    begin to get_user_topic_similarity...')
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
        topic_user_dict.setdefault(topics[i], dict())[user_dict[users[i]]] = props[i]

    # get user relationship matrix
    C = dict()  #记录用户相关矩阵
    N = dict()  #记录每个用户兴趣总值, 所有probability相加
    for i, i_users_prop in topic_user_dict.items():
        for u_prop in i_users_prop.items():  #字典
            u1 = u_prop[0]
            if u1 not in C:
                C[u1] = {}
            N[u1] = N.setdefault(u1, 0) + u_prop[1]
            for v_prop in i_users_prop.items():
                u2 = v_prop[0]
                if u1 == v_prop[0]:
                    continue
                C[u1][u2] = C[u1].setdefault(u2, 0) + min(u_prop[1], v_prop[1])

    #calcute final similirity matrix W based on user matrix
    W = dict()
    for u, related_users in C.items():
        for v, cuv in related_users.items():
            if (user_invert_dict[v] in W) and (user_invert_dict[u] in W[user_invert_dict[v]]):
                continue
            if user_invert_dict[u] not in W:
                W[user_invert_dict[u]] = dict()
            W.setdefault(user_invert_dict[u], dict())
            if user_invert_dict[v] not in W[user_invert_dict[u]]:
                W[user_invert_dict[u]][user_invert_dict[v]] = 0
            #print 'cuv = {} and sqrt = {},  {}'.format(cuv, N[u], N[v])
            #sim = cuv / math.sqrt(N[u] * N[v])
            #if sim != 1 and sim > 0.05:
            W[user_invert_dict[u]][user_invert_dict[v]] = cuv / math.sqrt(N[u] * N[v])

    log_cf.info('    finished get_user_topic_similarity...')
    return W


#计算由cf推荐的topic, 这些topic是用户没有点击过的
#相似度作为与邻居的权重; 推荐概率为sum(权重 * topic概率)/邻居数
#@input: user_topic_prop_dict ----{u1: {t1:0.1, t2:0.3, t4:0.1.. }, ...}
#        user_neighbours ----{u1:[(u2, 0.2), (u4, 0.05), ...], ...}
def get_potential_topic(user_topic_prop_dict, user_neighbours, model_v, time):
    log_cf.info('    begin to get_potential_topic...')
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
                    potential_utp_dict[u][tp[0]] = potential_utp_dict[u].setdefault(tp[0], 0) + sim * tp[1]

    user_potential_topic_sql = "insert into user_topic_cf (uid, model_v, topic_id, property, ctime) VALUES ({}, '{}', {}, {}, '{}')"
    if TEST_FLAG:
        us, ts, ps = [], [], []
        for item in potential_utp_dict:
            us.append(item[0])
            ts.append(item[1][0])
            ps.append(item[1][1])
        f = os.path.join(real_dir_path, 'data', 'final_recommend.csv')
        pd.DataFrame({'user':us, 'topic':ts, 'prop':ps}).to_csv(f, columns=('user', 'topic', 'prop'))
    else:
        conn, cursor = get_postgredb()
        for item in potential_utp_dict.items():
            u = item[0]
            for it in item[1].items():
                if it[1] > 0.05:
                    cursor.execute(user_potential_topic_sql.format(u, model_v, it[0], it[1], time))
        conn.commit()
        conn.close()
    log_cf.info('    finished get_potential_topic...')


################################################################################
# 整体流程
################################################################################
def get_user_topic_cf():
    log_cf.info('begin to calculate user_topic_cf...')
    time = datetime.datetime.now()
    # time_str = time.strftime('%Y-%m-%d-%H-%M-%S')
    model_v = get_newest_topic_v()
    # 读取user-topic-property
    user_topic_prop_dict, user_ids, topic_ids, props = coll_user_topics(model_v)
    # 计算neighbour
    user_neighbours = cal_neignbours(user_ids, topic_ids, props)
    # 计算neighbour推荐的topic
    get_potential_topic(user_topic_prop_dict, user_neighbours, model_v, time)
    log_cf.info("!!! calculate finished!")


clear_sql = "delete from user_topic_cf where ctime < now() - interval '1 day'"
clear_sql2 = "delete from user_similarity_cf where ctime < now() - interval '1 day'"
def clear_data():
    try:
        log_cf_clear_data.info('begin clear data...')
        conn, cursor = get_postgredb()
        cursor.execute(clear_sql)
        cursor.execute(clear_sql2)
        conn.commit()
        cursor.close()
        conn.close()
        log_cf_clear_data.info('finish clearing data...')
    except:
        pass


if __name__ == '__main__':
    coll_user_topics()





