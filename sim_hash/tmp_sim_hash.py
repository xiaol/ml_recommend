# -*- coding: utf-8 -*-
# @Time    : 17/6/2 下午4:30
# @Author  : liulei
# @Brief   : 旧新闻去重
# @File    : tmp_sim_hash.py
# @Software: PyCharm Community Edition
from util import doc_process
import traceback
import logging
import datetime
import os
import requests
from util.simhash import simhash, get_4_segments
from util.logger import Logger
from multiprocessing import Pool

real_dir_path = os.path.split(os.path.realpath(__file__))[0]

tmp_logger = Logger('tmp_sim_hash', os.path.join(real_dir_path, 'log/tmp_log.txt'))

def find_old_news():
    '''
    找到旧新闻, 按nid逆序返回
    :return:
    '''
    print 'begin to find nids to check...'
    nid_sql = "select nid from newslist_v2 where nid < 19127303 order by nid desc"
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(nid_sql)
    rows = cursor.fetchall()
    nids = [r[0] for r in rows]
    cursor.close()
    conn.close()
    print 'first of nid is {}'.format(nids[0])
    print 'len of nids is {}'.format(len(nids))
    return nids


hash_sql = "select ns.nid, hash_val from news_simhash ns inner join newslist_v2 nv on ns.nid=nv.nid where ns.nid <19127303 and ns.nid > {0} - 150000 and nv.state=0 " \
               "and (first_16='{1}' or second_16='{2}' or third_16='{3}' or fourth_16='{4}') and (first2_16='{5}' or second2_16='{6}' or third2_16='{7}' or fourth2_16='{8}') "
def get_news_interval(nid, h):
    '''
    按照id段取需要检查的新闻;
    :param nid:
    :param h:
    :return:
    '''
    fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__())
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(
        hash_sql.format(nid, fir, sec, thi, fou, fir2, sec2, thi2, fou2))
    rows = cursor.fetchall()
    nid_hv_list = []
    for r in rows:
        nid_hv_list.append((r[0], r[1]))
    conn.close()
    return nid_hv_list


'''
insert_same_sql = "insert into news_simhash_map (nid, same_nid, diff_bit, ctime) VALUES ({0}, {1}, {2}, '{3}')"
insert_news_simhash_sql = "insert into news_simhash (nid, hash_val, ctime, first_16, second_16, third_16, fourth_16, first2_16, second2_16, third2_16, fourth2_16) " \
                          "VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}')"
def cal_and_check_news_hash(nid_list):
    try:
        tmp_logger.info('begin to calculate simhash of {}'.format(' '.join(str(m) for m in nid_list)))
        t0 = datetime.datetime.now()
        conn, cursor = doc_process.get_postgredb()
        for nid in nid_list:
            words_list = doc_process.get_words_on_nid(nid)
            if len(words_list) < 10:
                continue
            h = simhash(words_list)
            check_list = get_news_interval(nid, h, 2)
            same_list = get_same_news(h, check_list, threshold=6)
            if len(same_list) > 0: #已经存在相同的新闻
                for n_dis in same_list:
                    n = n_dis[0]
                    diff_bit = n_dis[1]
                    if n != nid:
                        cursor.execute(insert_same_sql.format(nid, n, diff_bit, t0.strftime('%Y-%m-%d %H:%M:%S')))
                        del_nid_of_fewer_comment(nid, n)
            #else: #没有相同的新闻,将nid添加到news_hash
            t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__())
            cursor.execute(insert_news_simhash_sql.format(nid, h.__str__(), t, fir, sec, thi, fou, fir2, sec2, thi2, fou2))
            conn.commit()
        cursor.close()
        conn.close()
        t1 = datetime.datetime.now()
        tmp_logger.info('finish to calculate simhash. it takes {} s'.format(str((t1 - t0).total_seconds())))
    except:
        tmp_logger.error(traceback.format_exc())
'''


nid_sql = "select title, content from newslist_v2 where nid=%s "
def get_words_on_nid(nid):
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(nid_sql, [nid])
    rows = cursor.fetchall()
    word_list = []
    for row in rows:
        title = row[0]  #str类型
        content_list = row[1]
        txt = ''
        for content in content_list:
            if 'txt' in content.keys():
                txt += content['txt']
        total_txt = title + txt.encode('utf-8')
        word_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
    cursor.close()
    conn.close()
    return word_list


insert_news_simhash_sql2 = "insert into news_simhash_olddata (nid, hash_val, ctime, first_16, second_16, third_16, fourth_16, first2_16, second2_16, third2_16, fourth2_16) " \
                           "VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}')"
insert_same_sql = "insert into news_simhash_map (nid, same_nid, diff_bit, ctime) VALUES ({0}, {1}, {2}, '{3}')"
def cal_simhash_proc(nids):
    conn, cursor = doc_process.get_postgredb()
    n = 0
    for nid in nids:
        words_list = get_words_on_nid(nid)
        if len(words_list) < 10:
            continue
        h = simhash(words_list)
        t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__())
        cursor.execute(insert_news_simhash_sql2.format(nid, h.__str__(), t, fir, sec, thi, fou, fir2, sec2, thi2, fou2))
        conn.commit()
        n += 1
        if n % 1000 == 0:
            print '{} finished!'.format(n)
    cursor.close()
    conn.close()




def cal_simhash_old():
    conn, cursor = doc_process.get_postgredb_query()
    nids_sql = "select nid from newslist_v2 where nid < 13821715 and nid >4976433 order by nid"
    cursor.execute(nids_sql)
    rows = cursor.fetchall()
    nid_list = [r[0] for r in rows]
    print 'len of nids is {}'.format(len(nid_list))
    small_list = [nid_list[i:i + 10000] for i in range(0, len(nid_list), 10000)]
    from multiprocessing import Pool
    pool = Pool(30)
    for nids in small_list:
        pool.apply_async(cal_simhash_proc,
                         args=(nids, ))
    pool.close()
    pool.join()

    print '*****  all finished *****'
    cursor.close()
    conn.close()

from util import simhash
from sim_hash import del_nid_of_fewer_comment
def check_and_remove_proc(nids_info, pos, offset):
    hash_sql = "select ns.nid, hash_val from news_simhash  where ns.nid < {0} and ns.nid > {0} - 100000 " \
               "and (first_16='{1}' or second_16='{2}' or third_16='{3}' or fourth_16='{4}') and (first2_16='{5}' or second2_16='{6}' or third2_16='{7}' or fourth2_16='{8}') "
    k = pos
    leng = len(nids_info)
    check_up = min(leng, pos + offset)
    conn, cursor = doc_process.get_postgredb()
    t0 = datetime.datetime.now()
    while k < check_up:
        info = nids_info[k]
        if doc_process.get_news_online_state(info[0]): #已经下线
            continue
        hash_v = long(info[1])
        cursor.execute(hash_sql.format(info[0], info[2], info[3], info[4], info[5], info[6], info[7], info[8], info[9]))
        rows = cursor.fetchall()
        for r in rows:
            if doc_process.get_news_online_state(r[0]): #已经下线
                continue
            dis = simhash.dif_bit(hash_v, long(r[1]))
            if dis <= 6:
                cursor.execute(insert_same_sql.format(info[0], r[0], dis, t0.strftime('%Y-%m-%d %H:%M:%S')))
                del_nid_of_fewer_comment(info[0], r[0], log=tmp_logger)
        k += 1

    conn.commit()
    cursor.close()
    conn.close()


def check_and_remove_news():
    conn, cursor = doc_process.get_postgredb_query()
    print 'begin to check...'
    #t = datetime.datetime.now().strf
    #nids_sql = "select nid, hash_val, first_16, second_16, third_16, fourth_16, first2_16, second2_16, third2_16, fourth2_16 from news_simhash_olddata ns " \
    #           "inner join newslist_v2 nl on ns.nid=nl.nid where nl.ctime>now() - interval '30 day' "
    nids_sql = "select nid, hash_val, first_16, second_16, third_16, fourth_16, first2_16, second2_16, third2_16, fourth2_16 from news_simhash " \
               "where nid < 13821715" \
               #"ns " \
               #"inner join newslist_v2 nl on ns.nid=nl.nid where nl.ctime>now() - interval '30 day' "
    cursor.execute(nids_sql)
    rows = cursor.fetchall()
    nids_info = list(rows)
    print 'nids_sql finished! {} news find.'.format(len(nids_info))
    check_and_remove_proc(nids_info, 0, len(nids_info))
    '''
    pool = Pool(30)
    pos = 0
    while pos < len(nids_info):
        pool.apply_async(check_and_remove_proc, args=(nids_info, pos, pos))
        pos += 200000

    pool.close()
    pool.join()
    '''
    cursor.close()
    conn.close()
    print 'all finished*************'







