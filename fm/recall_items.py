# -*- coding: utf-8 -*-
import os
import sys
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(path)

from util.postgres import postgres_read_only as pg
from collections import OrderedDict
condition = " and nv.chid != 28 and nv.state=0 and (nv.rtype is null  or nv.rtype=0) "
select = "nv.nid, nv.docid, nv.title, nv.pname, nv.ptime, nv.purl, nv.chid, nv.collect, nv.concern, nv.un_concern, nv.comment, nv.style, nv.imgs,  nv.icon, nv.videourl, nv.duration, nv.thumbnail, nv.clicktimes, nv.tags"
dayWindow1 = " now()-interval'1 day' "

def recall_wilson_news(user_id, limit):
    dayWindow3 = " now()-interval'3 day' "
    table_name = "newsrecommendread_" + str(user_id % 100)

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
    wilson_dict = OrderedDict()
    for w in wilson_rows:
        wilson_dict[w['nid']] = dict(w)
        wilson_dict[w['nid']]['ptime'] = str(wilson_dict[w['nid']]['ptime'])
    return wilson_dict
    # TODO if wilson is empty , you get lucky


def recall_lda_kmeans_cf(user_id, limit):
    tablename1 = "newsrecommendread_" + str(user_id % 100)
    tablename2 = "newsrecommendforuser_" + str(user_id % 10)

    sql = '''select * from ( select {select} , 21 as rtype, 21 as logtype from newslist_v2 nv 
           where  not exists (select 1 from {tablename1} nr where nv.nid=nr.nid and nr.uid={uid}
           and nr.readtime>{dayWindow1}) and nv.nid in (select nid from {tablename2} where uid={uid} 
           and ctime>{dayWindow1} and sourcetype=1) and nv.ctime>{dayWindow1} {condition} limit {limitalgorithm})lda 
           union all select * from (select {select} , 21 as rtype, 22 as logtype from newslist_v2 nv 
           where  not exists (select 1 from {tablename1} nr where nv.nid=nr.nid 
           and nr.uid={uid} and nr.readtime>{dayWindow1}) and nv.nid in 
           (select nid from {tablename2} where uid={uid} and ctime>{dayWindow1} and sourcetype=2)
            and nv.ctime>{dayWindow1} {condition} limit {limitalgorithm})kmeans 
            union all select * from (select {select} , 21 as rtype, 27 as logtype from newslist_v2 nv 
            where  not exists (select 1 from {tablename1} nr where nv.nid=nr.nid and nr.uid={uid}
            and nr.readtime>{dayWindow1}) and nv.nid in (select nid from {tablename2} where uid={uid}
            and ctime>{dayWindow1} and sourcetype=3) 
            and nv.ctime>{dayWindow1} {condition} limit {limitalgorithm})cf '''

    sql = sql.format(select=select, tablename1=tablename1,
                     tablename2=tablename2, uid=user_id, dayWindow1=dayWindow1,
                     limitalgorithm=limit, condition=condition)
    lkc_rows = pg.query_dict_cursor(sql)
    lkc_dict = OrderedDict()
    for lkc in lkc_rows:
        lkc_dict[lkc['nid']] = dict(lkc)
        lkc_dict[lkc['nid']]['ptime'] = str(lkc_dict[lkc['nid']]['ptime'])
    return lkc_dict


def recall_hotnews(user_id, limit):
    pass


def recall_bigimg_video(user_id, limit):
    tablename = "newsrecommendread_" + user_id % 100
    sql =''' select * from (select nv.nid, nv.docid, nv.title, nv.pname, nv.ptime, nv.purl, 
          nv.chid, nv.collect, nv.concern, nv.un_concern, nv.comment, case when nr.bigimg>0 
          then (10+nr.bigimg) else nv.style end as style, array_to_string(nv.imgs, ',') as imgs,  
          nv.icon, nv.videourl, nv.duration, nv.thumbnail, nv.clicktimes, 
          array_to_string(nv.tags, ',') as tags, 999 as rtype, 25 as logtype from newslist_v2 nv 
          inner join newsrecommendlist nr on nv.nid=nr.nid where  not exists (select 1 from #$tablename nr 
          where nv.nid=nr.nid and nr.uid=$uid and nr.readtime>#$dayWindow3)  and nv.ctime>#$dayWindow3 \
          and nr.rtime>#$dayWindow3 and nr.status=1 and nr.chid=1 order by level desc, style desc, 
          rtime desc limit $limit)bigimage union all select * from (#$select , 6 as rtype, 6 as logtype 
          from blanknews_sortinglist bs inner join  newslist_v2  nv on nv.nid = bs.nid 
          where  not exists (select 1 from #$tablename nr where nv.nid=nr.nid and nr.uid=$uid 
          and nr.readtime> #$dayWindow1) and nv.ctime> #$dayWindow1  and bs.chid =44 
          order by bs.score desc  limit #$limit ) video'''



if __name__ == '__main__':
    wilson_dict = recall_wilson_news(33658617, 10)
    print "----hold on---------------"
