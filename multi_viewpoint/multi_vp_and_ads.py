# -*- coding: utf-8 -*-
# @Time    : 17/2/22 下午5:19
# @Author  : liulei
# @Brief   : 利用句子hash寻找多放观点和广告
# @File    : milti_vp_and_ads_detect.py
# @Software: PyCharm Community Edition
from util.doc_process import get_postgredb
################################################################################
# 多放观点的原则:  1. 两条新闻不来自同一源 2. 段落字数超过20  3. 相同的词达到60% 4. 不是重复新闻
# 广告的原则: 1.同一个源的文章的某一个重复的段落出现了5次以上
#      流程: 1. 取一条重复的句子,如果来自同一个源,则入广告库;若该源的该句子已在库中,次数加1
#            2. 次数超过5的均认为是广告
################################################################################
sql = "select nid1, nid2, sentence1, sentence2 from news_same_sentence_map " \
      "where nid1 in %s"
ch_sql = "select nid, pname from newslist_v2 where nid in %s"  #获取新闻源
def detect_multivp_and_ads(nid_list):
    conn, cursor = get_postgredb()
    cursor.execute(sql, (tuple(nid_list), ))
    rows = cursor.fetchall()
    all_nids = set()
    #查询这些nid的pname
    for r in rows:
        all_nids.add(r[0])
        all_nids.add(r[1])
    nids = []
    for i in all_nids:
        nids.append(i)
    cursor.execute(ch_sql, (tuple(nids), ))
    rows2 = cursor.fetchall()
    nid_pname_dict = {}
    for r in rows2:
        nid_pname_dict[r[0]] = r[1]

    for r in rows:
        nid1 = r[0]
        pname1 = nid_pname_dict[nid1]
        nid2 = r[1]
        pname2 = nid_pname_dict[nid2]
        if pname1 == pname2:  #同一个源,可能是广告
            pass
        else:    #进一步判断是否是多放观点
            pass

