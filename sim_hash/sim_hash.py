# -*- coding: utf-8 -*-
# @Time    : 17/2/4 下午2:46
# @Author  : liulei
# @Brief   : 
# @File    : sim_hash.py
# @Software: PyCharm Community Edition

from util import doc_process
import traceback
import logging
import datetime
import os
import requests
from util.simhash import simhash, get_4_segments, dif_bit
from util.logger import Logger
from multiprocessing import Pool

real_dir_path = os.path.split(os.path.realpath(__file__))[0]

logger = Logger('sim_hash', os.path.join(real_dir_path, 'log/log.txt'))
logger_sen = Logger('sim_hash_sen', os.path.join(real_dir_path, 'log/log_sen.txt'))  #根据sentence hash 去重

###########################################################################
#@brief :计算新闻的hash值.
#@input  : nid, int或str类型都可以
###########################################################################
def get_news_hash(nid_list):
    try:
        nid_hash_dict = {}
        for nid in nid_list:
            words_list = doc_process.get_words_on_nid(nid)
            h = simhash(words_list)
            nid_hash_dict[nid] = h.__long__()
        return nid_hash_dict
    except:
        logger.error(traceback.format_exc())
        return {}


hash_sql = "select ns.nid, hash_val from news_simhash ns inner join newslist_v2 nv on ns.nid=nv.nid where (ns.ctime > now() - interval '{0} day') and nv.state=0 " \
           "and (first_16='{1}' or second_16='{2}' or third_16='{3}' or fourth_16='{4}') and (first2_16='{5}' or second2_16='{6}' or third2_16='{7}' or fourth2_16='{8}') "
def get_news_interval(h, interval = 9999):
    '''
    找到一定时间内可能重复的新闻
    :param h:
    :param interval:
    :return:
    '''
    fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__())
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(hash_sql.format(interval, fir, sec, thi, fou, fir2, sec2, thi2, fou2))
    rows = cursor.fetchall()
    nid_hv_list = []
    for r in rows:
        nid_hv_list.append((r[0], r[1]))
    conn.close()
    return nid_hv_list


def get_old_news(interval=2):
    old_news_sql = "select ns.nid, hash_val from news_simhash ns " \
                   "inner join newslist_v2 nv on ns.nid=nv.nid " \
                   "where (ns.ctime > now() - interval '{0} day') and nv.state=0 " \
                   "and nv.chid != 44"
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(old_news_sql.format(interval))
    rows = cursor.fetchall()
    nids_hash_dict = dict()
    for r in rows:
        nids_hash_dict[r[0]] = long(r[1])
    cursor.close()
    conn.close()
    return nids_hash_dict


def cal_save_simhash_proc(nids):
    conn, cursor = doc_process.get_postgredb()
    for nid in nids:
        words_list = doc_process.get_words_on_nid(nid) #获取新闻的分词
        h = simhash(words_list) #本篇新闻的hash值
        t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__()) #获取hash值的分段
        cursor.execute(insert_news_simhash_sql.format(nid, h.__str__(), t, fir, sec, thi, fou, fir2, sec2, thi2, fou2))#记录新闻hash新闻
        conn.commit()
    cursor.close()
    conn.close()


def cal_save_simhash(nid_list):
    small_list = [nid_list[i:i + 5] for i in range(0, len(nid_list), 5)]
    pool = Pool(20)
    for nids in small_list:
        pool.apply_async(cal_save_simhash_proc, args=(nids,))

    pool.close()
    pool.join()
    del pool


def del_same_old_news(nid, nid_hash_dict):
    '''
        直接对比
    '''
    if nid not in nid_hash_dict:
        return
    t0 = datetime.datetime.now()
    #print t0
    #print '3'
    conn, cursor = doc_process.get_postgredb()
    t0 = datetime.datetime.now()
    #print t0
    #print '4'
    hash_val = nid_hash_dict[nid]
    for n, hv in nid_hash_dict.items():
        if n == nid:
            continue

        t0 = datetime.datetime.now()
        #print t0
        #print '    5'
        diff_bit = dif_bit(hash_val, long(hv))
        if diff_bit <= 6:
            offnid = del_nid_of_fewer_comment(nid, n)
            t0 = datetime.datetime.now()
            #print t0
            #print '    6'
            cursor.execute(insert_same_sql.format(nid, n, diff_bit, t0.strftime('%Y-%m-%d %H:%M:%S'), offnid)) #记录去重操作
            logger.inf('________ delete {}'.format(offnid))
            nid_hash_dict.pop(offnid)
            if offnid == nid: #新检测的新闻下线,则不再需要对比其他
                break

    conn.commit()
    cursor.close()
    conn.close()


def get_same_news(news_simhash, check_list, threshold = 3):
    '''
    获取与特定simhash相同的新闻
    :param news_simhash: 待检查的simhash
    :param check_list: 检查列表
    :param threshold: 阈值, 作为相同的判断条件
    :return:
    '''
    try:
        same_list = []
        for r in check_list:
            hv = r[1]
            dis = news_simhash.hamming_distance_with_val(int(hv))
            if dis <= threshold:  #存在相同的新闻
                same_list.append((r[0], dis))
                break
        return same_list
    except:
        logger.error(traceback.format_exc())


#去除广告的原则, 目前考虑图片数量,段落数量,评论数量
#得分越小, 越需要删除
def goal_to_del(contents, coment_num):
    img_num = 0
    para_num = 0
    for con in contents:
        para_num += 1
        if 'img' in con.keys():
            img_num += 1

    return img_num + para_num + 0.3*coment_num


################################################################################
#@brief : 删除重复的新闻
################################################################################
#get_comment_num_sql = 'select nid, comment, content from newslist_v2 where nid in ({0}, {1})'
get_comment_num_sql = 'select nv.nid, nv.comment, ni.content from newslist_v2 nv inner join info_news ni on nv.nid=ni.nid where nv.nid in ({0}, {1})'
recommend_sql = "select nid, rtime from newsrecommendlist where rtime > now() - interval '1 day' and nid in (%s, %s)"
#offonline_sql = 'update newslist_v2 set state=1 where nid={0}'
url = "http://114.55.142.40:9001/news_delete"
def del_nid_of_fewer_comment(nid, n, log=logger):
    try:
        conn, cursor = doc_process.get_postgredb_query()
        #先判断新闻是否已经被手工推荐。有则删除没有被手工推荐的新闻
        cursor.execute(recommend_sql, (nid, n))
        rs = cursor.fetchall()
        if len(rs) == 1: #一个被手工上线
            for r in rs:
                rnid = r[0]
            if rnid == n:
                del_nid = nid
                stay_nid = n
            else:
                del_nid = n
                stay_nid = nid
            #cursor.execute(offonline_sql.format(del_nid))
            #conn.commit()
            data = {}
            data['nid'] = del_nid
            response = requests.post(url, data=data)
            cursor.close()
            conn.close()
            log.info('{0} has been recommended, so offline {1}'.format(stay_nid, del_nid))
            return del_nid

        cursor.execute(get_comment_num_sql.format(nid, n))
        rows = cursor.fetchall()
        nid_goal = []
        for r in rows:
            nid_goal.append((r[0], goal_to_del(r[2], r[1])))  #计算两篇新闻的得分
        if len(nid_goal) == 0:  #查库失败, 直接删除旧新闻
            return n
        sorted_goal = sorted(nid_goal, key=lambda goal:goal[1])
        del_nid = sorted_goal[0][0]

        data = {}
        data['nid'] = del_nid
        response = requests.post(url, data=data)
        cursor.close()
        conn.close()
        log.info('{0} vs {1},  offline {2}'.format(nid, n, del_nid))
        return del_nid
    except Exception as e:
        log.error(traceback.format_exc())


################################################################################
#@brief : 计算新闻hash值,并且检测是否是重复新闻。如果重复,则删除该新闻
################################################################################
insert_same_sql = "insert into news_simhash_map (nid, same_nid, diff_bit, ctime, offline_nid) VALUES ({0}, {1}, {2}, '{3}', {4})"
insert_news_simhash_sql = "insert into news_simhash (nid, hash_val, ctime, first_16, second_16, third_16, fourth_16, first2_16, second2_16, third2_16, fourth2_16) " \
                          "VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}')"
def cal_and_check_news_hash(nid_list):
    try:
        #print '----'
        logger.info('begin to calculate {0} simhash of {1}'.format(len(nid_list), ' '.join(str(m) for m in nid_list)))
        t0 = datetime.datetime.now()
        #计算这些新闻的hash值并保存
        print  t0
        #print '0'
        cal_save_simhash(nid_list)
        t00 = datetime.datetime.now()
        print t00
        #print '1'

        nid_hash_dict = get_old_news(interval=2)
        print 'len of nid = {}'.format(len(nid_hash_dict))
        t00 = datetime.datetime.now()
        print t00
        #print '2'
        for nid in nid_list:
            del_same_old_news(nid, nid_hash_dict)
        '''
        conn, cursor = doc_process.get_postgredb()
        for nid in nid_list:
            words_list = doc_process.get_words_on_nid(nid) #获取新闻的分词
            if len(words_list) < 10: #文本过短不去重
                continue
            h = simhash(words_list) #本篇新闻的hash值
            check_list = get_news_interval(h, 2)  #获取要对比的新闻列表,目前取2天内的新闻做重复性检查
            same_list = get_same_news(h, check_list, threshold=6) #重复新闻
            if len(same_list) > 0: #已经存在相同的新闻
                for n_dis in same_list:
                    n = n_dis[0]
                    diff_bit = n_dis[1]
                    if n != nid:
                        off_nid = del_nid_of_fewer_comment(nid, n) #下线一篇新闻
                        cursor.execute(insert_same_sql.format(nid, n, diff_bit, t0.strftime('%Y-%m-%d %H:%M:%S'), off_nid)) #记录去重操作
            #else: #没有相同的新闻,将nid添加到news_hash
            t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__()) #获取hash值的分段
            cursor.execute(insert_news_simhash_sql.format(nid, h.__str__(), t, fir, sec, thi, fou, fir2, sec2, thi2, fou2))#记录新闻hash新闻
            conn.commit()
        cursor.close()
        conn.close()
        '''
        t1 = datetime.datetime.now()
        logger.info('finish to calculate simhash. it takes {} s'.format(str((t1 - t0).total_seconds())))
    except:
        logger.error(traceback.format_exc())


def is_news_same(nid1, nid2, same_t=3):
    try:
        w1 = doc_process.get_words_on_nid(nid1)
        w2 = doc_process.get_words_on_nid(nid2)
        h1 = simhash(w1)
        h2 = simhash(w2)
        print h1.hamming_distance(h2)
        if h1.hamming_distance(h2) > same_t:
            return False
        return True
    except:
        raise


#依据sentence_hash放到队列中的待查新闻
def check_same_news(nid1, nid2):
    conn, cursor = doc_process.get_postgredb()
    check_state = "select state from newslist_v2 where nid in ({}, {}) and state=0"
    cursor.execute(check_state.format(nid1, nid2))
    rs = cursor.fetchall()
    if len(list(rs)) < 2:
        return
    words_list1 = doc_process.get_words_on_nid(nid1) #获取新闻的分词
    words_list2 = doc_process.get_words_on_nid(nid2) #获取新闻的分词
    h1 = simhash(words_list1) #本篇新闻的hash值
    h2 = simhash(words_list2) #本篇新闻的hash值
    diff_bit = h1.hamming_distance(h2)
    if diff_bit > 12: #大于12, 认为不可能是同一篇新闻
        return
    title_sql = "select title from newslist_v2 where nid in ({}, {})"
    cursor.execute(title_sql.format(nid1, nid2))
    rows = cursor.fetchall()
    titles = [r[0] for r in rows]
    if doc_process.get_sentence_similarity(titles[0], titles[1]) > 0.3: #标题相似性大于0.3
        off_nid = del_nid_of_fewer_comment(nid1, nid2, log=logger_sen)
        t0 = datetime.datetime.now()
        cursor.execute(insert_same_sql.format(nid1, nid2, diff_bit, t0.strftime('%Y-%m-%d %H:%M:%S'), off_nid)) #记录去重操作
    conn.commit()
    cursor.close()
    conn.close()


import jieba
if __name__ == '__main__':

    nid_list = [11952459, 11952414]
    #print is_news_same(11952760, 11963937, 3)
    print is_news_same(3235506, 3375849, 4)
    print is_news_same(3211559, 3212267, 4)
    print is_news_same(3248729, 3247245, 4)
    print is_news_same(12119757, 12119757, 4)
    print is_news_same(15134277, 15135383, 4)
    #cal_and_check_news_hash(nid_list)
    #w1 = doc_process.get_words_on_nid(11580728)
    #w2 = doc_process.get_words_on_nid(11603489)
    #h1 = simhash(w1)
    #h2 = simhash(w2)
    #print 100 * h2.similarity(h1)
    #print h1.hamming_distance(h2), "bits differ out of", h1.hashbits




