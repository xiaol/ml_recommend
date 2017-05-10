# -*- coding: utf-8 -*-
# @Time    : 16/12/1 下午5:23
# @Author  : liulei
# @Brief   : 
# @File    : topic_model_doc_process.py
# @Software: PyCharm Community Edition
import os
from util import doc_process
from util import logger

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_prep = logger.Logger('lda_preprocess', os.path.join(real_dir_path,  'log/log_preprocess.txt'))

total_topic_num = 500000
channel_for_topic = ['科技', '外媒', '社会', '财经', '体育', '汽车', '国际', '时尚', '探索', '科学',
                     '娱乐', '养生', '育儿', '股票', '互联网', '美食', '健康', '影视', '军事', '历史',
                     '故事', '旅游', '美文', '萌宠', '游戏']
channel_for_topic = ['科技', '社会', '财经', '体育', '汽车', '国际']
channel_for_topic = ['体育', '社会', '科技', '国际', '娱乐']
channel_for_topic_dict = {'6':'体育', '2':'社会', '4':'科技', '9':'国际', '3':'娱乐'}
#需要进行额外判断新闻topic的频道,这些频道最终使用channel_for_topic的model进行预测
extra_channel_for_topic = ['点集', '自媒体']

channel_for_topic = ['科技', '外媒', '社会', '财经', '体育', '汽车', '国际', '时尚', '探索', '科学',
                     '娱乐', '养生', '育儿', '股票', '互联网', '美食', '健康', '影视', '军事', '历史',
                     '故事', '旅游', '美文', '萌宠', '游戏']
#channel_topic_dict = {'科技': 300, '外媒': 200, '社会':500, '财经':100, '汽车':200, '国际':300,
#'时尚':200, '探索':}


channle_sql ='SELECT a.title,a.content, a.nid, c.cname \
FROM newslist_v2 a \
INNER JOIN (select * from channellist_v2 where "cname"=%s) c \
ON \
a."chid"=c."id" ORDER BY nid desc DESC LIMIT %s'

#准备新闻数据
def collectNews(category, news_num, min_len=100):
    logger_prep.info('start to collect {} ......'.format(category))
    with open(real_dir_path+'/data/'+category, 'w') as f:  #清空
        f.close()
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(channle_sql, [category, news_num])
    rows = cursor.fetchall()
    for row in rows:
        title = row[0]
        content_list = row[1]
        txt = ''
        for content in content_list:
            if 'txt' in content.keys():
                txt += content['txt'].encode('utf-8')
        total_txt = title + txt
        total_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
        if len(total_list) < min_len:
            continue
        #根据tfidf进行二次筛选
        total_list = doc_process.jieba_extract_keywords(' '.join(total_list), min(50, len(total_list)/5))
        with open(real_dir_path+'/data/'+category, 'a') as f:
            for w in total_list:
                f.write(w.encode('utf-8') + ' ')
            f.write('\n')

    conn.close()


def coll_news_for_channles(news_num=100000):
    import multiprocessing as mp
    procs = []
    for chanl in channel_for_topic:
        coll_proc = mp.Process(target=collectNews, args=(chanl, news_num, 100))
        coll_proc.start()
        procs.append(coll_proc)
    for i in procs:
        i.join()
    print 'coll_news_for_channles finished!'




