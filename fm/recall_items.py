# -*- coding: utf-8 -*-
import os
import sys
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(path)

from util.postgres import postgres_read_only as pg


def recall_wilson_news(user_id, limit):
    dayWindow1 = " now()-interval'1 day' "
    dayWindow3 = " now()-interval'3 day' "
    table_name = "newsrecommendread_" + str(user_id % 100)
    condition = " and nv.chid != 28 and nv.state=0 and (nv.rtype is null  or nv.rtype=0) "
    select = "nv.nid, nv.docid, nv.title, nv.pname, nv.ptime, nv.purl, nv.chid, nv.collect, nv.concern, nv.un_concern, nv.comment, nv.style, nv.imgs,  nv.icon, nv.videourl, nv.duration, nv.thumbnail, nv.clicktimes, nv.tags"

    sql = '''
      select {select}, 0 as rtype, 0 as logtype
      from blanknews_sortinglist as bs
      left join newslist_v2 nv
      on nv.nid = bs.nid
      where  not exists (select 1 from {tablename} nr where nv.nid=nr.nid and nr.uid={uid} and nr.readtime>{readtime}) and nv.ctime>{createtime}  {condition} and nv.imgs is not null
      order by bs.score desc
      limit {limit}
    '''
    sql = sql.format(select=select, tablename=table_name, uid=user_id, readtime=dayWindow1,
               createtime=dayWindow1, condition=condition, limit=limit)
    wilson_rows = pg.query_dict_cursor(sql)
    wilson_dict = {}
    for w in wilson_rows:
        wilson_dict[w['nid']] = dict(w)
        wilson_dict[w['nid']]['ptime'] = str(wilson_dict[w['nid']]['ptime'])
    return wilson_dict
    # TODO if wilson is empty , you get lucky


def recall_lda(user_id, limit):
    lda_dict = {}
    return lda_dict


def recall_kmeans(user_id, limit):
    pass


def recall_hotnews(user_id, limit):
    pass


def recall_bigimg(user_id, limit):
    pass

if __name__ == '__main__':
    wilson_dict = recall_wilson_news(33658617, 10)
    print "----hold on---------------"
