# -*- coding: utf-8 -*-
# @Time    : 17/2/13 上午10:09
# @Author  : liulei
# @Brief   : 
# @File    : sentence_hash.py
# @Software: PyCharm Community Edition
#from util  import doc_process
from util.doc_process import get_postgredb
from util.doc_process import get_postgredb_query
from util.doc_process import filter_html_stopwords_pos
from util.doc_process import filter_tags
from util.doc_process import get_nids_sentences
import subject_queue

from util import simhash
import datetime
import os
import traceback
from multiprocessing import Pool
import jieba
from util import logger

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_9965 = logger.Logger('9965', os.path.join(real_dir_path,  'log/log_9965.txt'))
logger_9966 = logger.Logger('9966', os.path.join(real_dir_path,  'log/log_9966.txt'))


channel_for_multi_vp = ('科技', '外媒', '社会', '财经', '体育', '国际',
                        '娱乐', '养生', '育儿', '股票', '互联网', '健康',
                        '影视', '军事', '历史', '点集', '自媒体')
exclude_chnl = ['搞笑', '趣图', '美女', '萌宠', '时尚', '美食', '美文', '奇闻', '美食',
                '旅游', '汽车', '游戏', '科学', '故事', '探索']


insert_sentence_hash = "insert into news_sentence_hash_cache (nid, sentence, sentence_id, hash_val, first_16, second_16, third_16, fourth_16, ctime, first2_16, second2_16, third2_16, fourth2_16) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
query_sen_sql = "select ns.nid, ns.hash_val from news_sentence_hash_cache ns inner join newslist_v2 nl on ns.nid=nl.nid " \
                "where (first_16=%s or second_16=%s or third_16=%s or fourth_16=%s) and " \
                "(first2_16=%s or second2_16=%s or third2_16=%s or fourth2_16=%s) and " \
                "nl.state=0 group by ns.nid, ns.hash_val "
query_sen_sql_interval = "select ns.nid, ns.hash_val from news_sentence_hash_cache ns inner join newslist_v2 nl on ns.nid=nl.nid " \
                         "where (first_16=%s or second_16=%s or third_16=%s or fourth_16=%s) and " \
                         "(first2_16=%s or second2_16=%s or third2_16=%s or fourth2_16=%s) and " \
                         "(nl.ctime > now() - interval '%s day') and nl.state=0 group by ns.nid, ns.hash_val "
insert_same_sentence = "insert into news_same_sentence_map (nid1, nid2, sentence1, sentence2, ctime) VALUES (%s, %s, %s, %s, %s)"
s_nid_sql = "select distinct nid from news_sentence_hash_cache "
def get_exist_nids():
    conn, cursor = get_postgredb_query()
    cursor.execute(s_nid_sql)
    rows = cursor.fetchall()
    nid_set = set()
    for r in rows:
        nid_set.add(r[0])
    conn.close()
    return nid_set



################################################################################
#@brief : 读取相同的新闻
################################################################################
same_sql = "select nid, same_nid from news_simhash_map where (nid in %s) or (same_nid in %s) "
def get_relate_same_news(nid_set):
    if len(nid_set) == 0:
        return dict()
    conn, cursor = get_postgredb_query()
    nid_tuple = tuple(nid_set)
    cursor.execute(same_sql, (nid_tuple, nid_tuple))
    same_dict = {}
    rows = cursor.fetchall()
    for r in rows:
        if r[0] not in same_dict.keys():
            same_dict[r[0]] = []
        if r[1] not in same_dict.keys():
            same_dict[r[1]] = []
        same_dict[r[0]].append(r[1])
        same_dict[r[1]].append(r[0])

    conn.close()
    return same_dict


################################################################################
#@brief: 检查句子是否是广告   如果已经被判定是广告,则不再判断有无重复
################################################################################
check_ads_sql = "select ads_sentence, hash_val, special_pname from news_ads_sentence where " \
                "(first_16=%s or second_16=%s or third_16=%s or four_16=%s) and" \
                "(first2_16=%s or second2_16=%s or third2_16=%s or four2_16=%s) "
def is_sentence_ads(hash_val, fir_16, sec_16, thi_16, fou_16, fir2_16, sec2_16, thi2_16, fou2_16, pname):
    conn, cursor = get_postgredb_query()
    cursor.execute(check_ads_sql, (fir_16, sec_16, thi_16, fou_16, fir2_16, sec2_16, thi2_16, fou2_16))
    rows = cursor.fetchall()
    for r in rows:
        if hash_val.hamming_distance_with_val(long(r[1])) <= 3:
            exist = False
            if r[2]:
                spnames = r[2].split(',')
                if len(spnames) == 0 or (pname in spnames):
                    exist = True
            else:
                exist = True
            if exist:
                conn.close()
                return True
    conn.close()
    return False


#判断两个专题是否需要合并
def should_subject_merge(sub1, sub2):
    if sub1[0] == sub2[0] or sub1[1] == sub2[1]: #关键句子或新闻列表一样
        return True
    sub1_nids = set(sub1[1])
    sub2_nids = set(sub2[1])
    same_nids = sub1_nids & sub2_nids
    if (float(len(same_nids)) >= 0.5 * len(sub1_nids)) or \
       (float(len(same_nids)) >= 0.5 * len(sub2_nids)):
        return True
    else:
        return False


#合并两个专题
def merge_subject(sub1, sub2):
    new_sub_sents = []
    new_sub_sents.extend(sub1[0])
    new_sub_sents.extend(sub2[0])
    new_sub_nids = list(set(sub1[1]) | set(sub2[1]))
    return [new_sub_sents, new_sub_nids]


#多个专题的合并
'''
def merge_subs(subs_list):
    new_subs = []
    for i in xrange(len(subs_list)):
        finished = False
        for j in xrange(len(new_subs)):
            if should_subject_merge(subs_list[i], new_subs[j]):
                new_subs[j] = merge_subject(subs_list[i], new_subs[j])
                finished = True
                break
        if finished:
            break
        new_subs.append(subs_list[i])
    return new_subs
'''


################################################################################
#@brief : 合并专题
#@ step: 1.合并nids完全相同的专题  2.nid不同的专题, nid数<=4的不与其他的专题合并
################################################################################
def merge_subs(subs_list):
    new_subs = []
    for i in xrange(len(subs_list)):
        #检查nid完全相同的专题
        same_nids = False
        for k in xrange(len(new_subs)):
            if len(set(subs_list[i][1]) ^ set(new_subs[k][1])) == 0:
                new_subs[k] = merge_subject(subs_list[i], new_subs[k])
                same_nids = True
        if same_nids:
            continue


        if len(subs_list[i][1]) <= 4:  #新闻个数小于4个不合并
            new_subs.append(subs_list[i])
            continue
        merg = False
        for j in xrange(len(new_subs)):
            if len(new_subs[j][1]) <= 4:  #不能和新闻数小于4的专题合并
                continue
            if should_subject_merge(subs_list[i], new_subs[j]):
                new_subs[j] = merge_subject(subs_list[i], new_subs[j])
                merg = True
        if not merg:
            new_subs.append(subs_list[i])
    return new_subs


get_pname = "select pname, chid, ctime, nid from info_news where nid in %s"
same_sql2 = "select sentence from news_sentence_hash_cache where nid=%s and hash_val=%s"
ads_insert = "insert into news_ads_sentence (ads_sentence, hash_val, ctime, first_16, second_16, third_16, four_16, first2_16, second2_16, third2_16, four2_16, nids, state, special_pname) " \
             "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
multo_vp_insert_sql = "insert into news_multi_vp (nid1, sentence1, nid2, sentence2, ctime, ctime1, ctime2) values (%s, %s, %s, %s, %s, %s, %s)"
################################################################################
#@brief: 计算子进程
################################################################################
def cal_process(nid_set, log=None, same_t=3, news_interval=3, same_dict = {}):
    log = logger_9965
    log.info('there are {} news to calulate'.format(len(nid_set)))
    ttt1 = datetime.datetime.now()
    try:
        nid_sents_dict, nid_para_links_dict, nid_pname_dict = get_nids_sentences(nid_set)
        kkkk = 0
        for item in nid_sents_dict.items(): #每条新闻
            #存放专题, 每个元素包含关键句和新闻id两个列表
            #例如[[['abc', 'aaa'], [123, 231]], [['bcd', 'bbb'], [542, 126]] ]
            subject_sentence_nids = []
            kkkk += 1
            n = 0
            nid = item[0]
            log.info('    cal {} sentences...'.format(nid))
            #log.info('--- consume :{}'.format(nid))
            t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            para_sent_dict = item[1]

            sen_len = 0   #文章总句子数目
            for pa in para_sent_dict.items(): #每个段落
                sen_len += len(pa[1])
            for pa in para_sent_dict.items():
                para_num = pa[0]  #段落号
                sents = pa[1]
                conn, cursor = get_postgredb()
                conn_query, cursor_query = get_postgredb_query()
                for s in sents:  #每个句子
                    n += 1
                    #ts1 = datetime.datetime.now()
                    #print '-------1'
                    #print ts1
                    str_no_html, wl = filter_html_stopwords_pos(s, False, True, True, False)
                    #if len(wl) == 0 or len(str_no_html) <= 2: #去除一个字的句子,因为有很多是特殊字符
                    #if len(wl) == 0 or len(str_no_html) <= 15: #去除一个字的句子,因为有很多是特殊字符
                    #if len(wl) == 10 or len(str_no_html) <= 15: #去除一个字的句子,因为有很多是特殊字符
                    if len(wl) <= 10 : #去除一个字的句子,因为有很多是特殊字符
                        continue
                    #ts2 = datetime.datetime.now()
                    #print '-------2'
                    #print ts2
                    h = simhash.simhash(wl)
                    check_exist_sql = "select nid from news_sentence_hash_cache where nid=%s and hash_val=%s" #该新闻中已经有这个句子,即有重复句子存在
                    cursor_query.execute(check_exist_sql, (nid, h.__str__()))
                    #ts3 = datetime.datetime.now()
                    #print '-------3'
                    #print ts3
                    if len(cursor_query.fetchall()) != 0:
                        #log.info('sentence has existed in this news: {}'.format(str_no_html.encode("utf-8")))
                        continue
                    fir, sec, thi, fou, fir2, sec2, thi2, fou2 = simhash.get_4_segments(h.__long__())
                    if is_sentence_ads(h, fir, sec, thi, fou, fir2, sec2, thi2, fou2, nid_pname_dict[nid]):  #在广告db内
                        #  删除广告句子
                        #log.info('find ads of {0}  : {1} '.format(nid, str_no_html.encode("utf-8")))
                        continue
                    #ts4 = datetime.datetime.now()
                    #print '-------4'
                    #print ts4
                    cursor_query.execute(query_sen_sql_interval, (str(fir), str(sec), str(thi), str(fou), str(fir2), str(sec2), str(thi2), str(fou2), news_interval))
                    #print cursor.mogrify(query_sen_sql_interval, (str(fir), str(sec), str(thi), str(fou), str(fir2), str(sec2), str(thi2), str(fou2), news_interval))
                    rows = cursor_query.fetchall()  #所有可能相同的段落
                    #print 'len of potential same sentence is {}'.format(len(rows))
                    if len(rows) == 0:  #没有相似的句子
                        #将所有句子入库
                        cursor.execute(insert_sentence_hash, (nid, str_no_html, n, h.__str__(), fir, sec, thi, fou, t, fir2, sec2, thi2, fou2))
                        #logger_9965.info('len of potential same sentence is 0')
                        continue
                    #else:
                        #logger_9965.info('len of potential same sentence is {}'.format(len(rows)))

                    #ts5 = datetime.datetime.now()
                    #print '-------5'
                    #print ts5
                    same_sentence_sql_para = []
                    nids_for_ads = set()
                    for r in rows:
                        #if len(nids_for_ads) >= 15:
                            #break
                        #距离过大或者是同一篇新闻
                        if h.hamming_distance_with_val(long(r[1])) > same_t or (nid in same_dict.keys() and r[0] in same_dict[nid]) or nid == r[0]:
                            #logger_9965.info('distance is too big or same news of {} and {}'.format(nid, r[0]))
                            continue
                        cursor_query.execute(same_sql2, (r[0], r[1]))
                        rs = cursor_query.fetchall()
                        for r2 in rs:
                            sen = r2[0].decode('utf-8')
                            sen_without_html = filter_tags(sen)
                            if len(sen) == 1 or len(sen_without_html) > len(str_no_html)*1.5 or len(str_no_html) > len(sen_without_html)*1.5:
                                #logger_9965.info('sentence len mismatch: {} ----{}'.format(str_no_html.encode('utf-8'), sen_without_html))
                                continue
                            wl1 = jieba.cut(str_no_html)
                            set1 = set(wl1)
                            l1 = len(set1)
                            wl2 = jieba.cut(sen_without_html)
                            set2 = set(wl2)
                            set_same = set1 & set2
                            l2 = len(set2)
                            l3 = len(set_same)
                            if l3 < max(l1, l2) * 0.6:  #相同比例要达到0.6
                                continue
                            nids_for_ads.add(str(r[0]))
                            same_sentence_sql_para.append((nid, r[0], str_no_html, sen, t))
                            #cursor.execute(insert_same_sentence, (nid, r[0], str_no_html, sen, t))
                            #print cursor.mogrify(insert_same_sentence, (nid, r[0], str_no_html, sen_without_html, t))
                    #ts6 = datetime.datetime.now()
                    #print '-------6'
                    #print ts6
                    if len(nids_for_ads) == 0:  #没有潜在相同的句子; 这些句子先做广告检测
                        cursor.execute(insert_sentence_hash, (nid, str_no_html, n, h.__str__(), fir, sec, thi, fou, t, fir2, sec2, thi2, fou2))
                        conn.commit()
                        continue

                    is_new_ads = False
                    not_ads_but_ignore = False   #不是广告,但需要忽略计算重复
                    PNAME_T = 3
                    nid_pn = {}
                    pname_set = set()
                    chid_set = set()
                    ctime_list = []
                    #print cursor.mogrify(get_pname, (tuple(nids_for_ads),))
                    cursor_query.execute(get_pname, (tuple(nids_for_ads),))
                    rows2 = cursor_query.fetchall()
                    for rk in rows2:
                        pname_set.add(rk[0])
                        chid_set.add(rk[1])
                        ctime_list.append(rk[2])
                        nid_pn[rk[3]] = rk[0]
                    if len(nids_for_ads) / float(len(pname_set)) > 3: #2017.06.13 添加
                        is_new_ads = True
                    if len(nids_for_ads) >= 10:
                        #先处理同源潜在广告
                        if len(pname_set) <= PNAME_T or (len(pname_set) > 5 and len(chid_set) < 4):
                            #if n > sen_len * .2 and n < sen_len * .8:
                            if float(n) < float(sen_len * .2) or float(n) > float(sen_len * .8):
                                min_time = ctime_list[0]
                                max_time = ctime_list[0]
                                for kkk in xrange(1, len(ctime_list)):
                                    if ctime_list[kkk] > max_time:
                                        max_time = ctime_list[kkk]
                                    if ctime_list[kkk] < min_time:
                                        min_time = ctime_list[kkk]
                                if (max_time - min_time).days > 2:  #不是两天内的热点新闻
                                    is_new_ads = True
                            '''
                            nid_links = nid_para_links_dict[nid]
                            sum_own_links = 0  #有链接的段落数
                            for kk in xrange(para_num, len(nid_links)):
                                if len(nid_links[kk]):
                                    sum_own_links += 1
                            if sum_own_links > (len(nid_links) - para_num) * 0.8: #后面的链接很多,认为是广告
                                is_new_ads = True
                        elif len(pname_set) > 5 and len(chid_set) < 4:   #来自多个源, 看是否集中在几个频道,如果是,则认为是广告
                            #需要判断这些新闻入库时间不集中在3天内,否则可能不是广告
                            min_time = ctime_list[0]
                            max_time = ctime_list[0]
                            for kkk in xrange(1, len(ctime_list)):
                                if ctime_list[kkk] > max_time:
                                    max_time = ctime_list[kkk]
                                if ctime_list[kkk] < min_time:
                                    min_time = ctime_list[kkk]
                            if (max_time - min_time).days > 2:  #不是三天内的热点新闻
                                is_new_ads = True
                             '''
                        else:
                            not_ads_but_ignore = True
                    #ts7 = datetime.datetime.now()
                    #print '-------7'
                    #print ts7
                    nids_str = ','.join(nids_for_ads)
                    if is_new_ads:  #是否是新广告
                        if len(pname_set) <= PNAME_T:  #源
                            pname_str = ','.join(pname_set)
                        else:
                            pname_str = ""
                        cursor.execute(ads_insert, (str_no_html, h.__str__(), t, fir, sec, thi, fou, fir2, sec2, thi2, fou2, nids_str, 0, pname_str))
                        #log.info('find new ads : {0}'.format(str_no_html.encode("utf-8")))
                    else:
                        #if len(same_sentence_sql_para) < 5:  #检测出过多的相同句子,又不是广告, 可能是误判, 不处理
                        if not_ads_but_ignore:  #相同的句子过多,认为是误判, 加入广告数据库,但state=1,即不是真广告,这样可以在下次碰到时减少计算
                            cursor.execute(ads_insert, (str_no_html, h.__str__(), t, fir, sec, thi, fou, fir2, sec2, thi2, fou2, nids_str, 1, "" ))
                        else:
                            cursor.executemany(insert_same_sentence, same_sentence_sql_para)  #有效的重复句子
                            #log.info('get same sentence map :{}'.format(str_no_html.encode('utf-8')))
                            #多放观点  1. 句子长度>30.  2 不同源  3. 去除首尾
                            if len(str_no_html) > 15 and n > 2 and (n < sen_len-2):
                            #if len(str_no_html) > 15:
                                sub_nids_set = set()
                                for same in same_sentence_sql_para:
                                    nn = same[1]  #nid
                                    if nid_pname_dict[nid] != nid_pn[nn]:
                                        ctime_sql = "select nid, ctime from info_news where nid = %s or nid=%s"
                                        cursor_query.execute(ctime_sql, (same[0], same[1]))
                                        ctimes = cursor_query.fetchall()
                                        ctime_dict = {}
                                        for ct in ctimes:
                                            ctime_dict[str(ct[0])] = ct[1]
                                        cursor.execute(multo_vp_insert_sql, (str(same[0]), same[2], str(same[1]), same[3], t, ctime_dict[str(same[0])], ctime_dict[str(same[1])]))
                                        log.info('      get multi viewpoint :{}'.format(str_no_html.encode('utf-8')))
                                        sub_nids_set.add(same[0])
                                        sub_nids_set.add(same[1])
                                        subject_queue.product_simhash2((same[0], same[1]))
                                #log.info("num of mvp is {}".format(sub_nids_set))
                                if len(sub_nids_set) >= 2:  ## 专题新闻入队列
                                    log.info('      generate subject for {}'.format(sub_nids_set))
                                    #for i in sub_nids_set:
                                    #    subject_nids.add(i)
                                    key_sents = [str_no_html.encode('utf-8'), ]
                                    sub_nids = []
                                    for i in sub_nids_set:
                                        sub_nids.append(i)
                                    subject_sentence_nids.append([key_sents, sub_nids])
                                    #subject_queue.product_subject(tuple(nid_set))

                    #将所有段落入库
                    cursor.execute(insert_sentence_hash, (nid, str_no_html, n, h.__str__(), fir, sec, thi, fou, t, fir2, sec2, thi2, fou2))
                conn.commit()
                cursor.close()
                conn.close()
                cursor_query.close()
                conn_query.close()
            if len(subject_sentence_nids) > 0 and len(subject_sentence_nids) < 3:
                #log.info("before merge : {}".format(subject_sentence_nids))
                subs = merge_subs(subject_sentence_nids)
                #log.info("after merge : {}".format(subs))
                for sub in subs:
                    subject_queue.product_subject(sub)
                #log.info('generate subject for {} ------ {}'.format(nid, subject_nids))
                #subject_queue.product_subject(tuple(subject_nids))

        ttt2 = datetime.datetime.now()
        log.info('it takes {}'.format((ttt2-ttt1).total_seconds()))
        del nid_sents_dict
        del nid_para_links_dict
    except:
        log.exception(traceback.format_exc())


#供即时计算
def coll_sentence_hash_time(nid_list):
    #nid_set = set(nid_list)
    # arr是被分割的list，n是每个chunk中含n元素。
    try:
        from util.doc_process import keep_nids_based_cnames
        nid_list = keep_nids_based_cnames(tuple(nid_list), channel_for_multi_vp)
        small_list = [nid_list[i:i + 5] for i in range(0, len(nid_list), 5)]
        pool = Pool(20)
        same_dict = get_relate_same_news(set(nid_list))
        for nid_set in small_list:
            pool.apply_async(cal_process, args=(set(nid_set), None, 3, 2, same_dict))

        pool.close()
        pool.join()
    except:
        logger_9965.exception(traceback.format_exc())
    logger_9965.info("Congratulations! Finish to collect sentences.")


cal_sql = "select nid from info_news limit %s offset %s"
cal_sql2 ="SELECT a.nid \
FROM info_news a \
RIGHT OUTER JOIN (select * from channellist_v2 where cname not in %s) c \
ON \
a.chid =c.id where (a.ctime > now() - interval '2 day') and a.state=0 LIMIT %s offset %s"
ignore_cname = ("美女", "帅哥", "搞笑", "趣图", "视频")

def coll_sentence_hash():
    logger_9965.info("Begin to collect sentence...")
    exist_set = get_exist_nids()
    limit = 10000
    offset = 10000
    pool = Pool(30)
    while True:
        conn, cursor = get_postgredb_query()
        cursor.execute(cal_sql2, (ignore_cname, limit, offset))
        rows = cursor.fetchall()
        conn.close()
        offset += limit
        if len(rows) == 0:
            break
        all_set = set()
        for r in rows:
            all_set.add(r[0])
        need_to_cal_set = all_set - exist_set
        if len(need_to_cal_set) == 0:
            continue
        same_dict = get_relate_same_news(need_to_cal_set)
        pool.apply_async(cal_process, args=(need_to_cal_set, None, 3, 3, same_dict)) #相同的阈值为3; 取2天内的新闻

    pool.close()
    pool.join()

    logger_9965.info("Congratulations! Finish to collect sentences.")


#将3天前的数据从news_sentence_hash_cache 移动到news_sentenct_hash
move_sentenct_sql = "insert into news_sentence_hash select * from news_sentence_hash_cache " \
                    "where ctime <  to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss') - interval '3 day' "
del_sentenct_sql = "delete from news_sentence_hash_cache " \
                   "where ctime < to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss') - interval '3 day'"
logger_9963 = logger.Logger('9963', os.path.join(real_dir_path,  'log/log_9963.txt'))
def move_sentence_data():
    try:
        nt = datetime.datetime.now()
        t = nt.strftime('%Y-%m-%d %H:%M:%S')
        logger_9963.info('move_sentence_data--- {}'.format(t))
        conn, cursor = get_postgredb()
        cursor.execute(move_sentenct_sql, (t, ))
        logger_9963.info('move finished')
        cursor.execute(del_sentenct_sql, (t, ))
        conn.commit()
        conn.close()
        nt2 = datetime.datetime.now()
        logger_9963.info('finished to move_sentence_data. it takes {} s'.format((nt2 - nt).total_seconds()))
    except:
        logger_9963.info(traceback.format_exc())


