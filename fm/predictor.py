# -*- coding: utf-8 -*-


import als_solver
import etl_item_data
import etl_user_data
import etl_sample
import numpy as np
import scipy.sparse as sp
import operator
from util.redis_connector import redis_ali
import json
from news import News
import random
import datetime
import time
import argparse


feedSuffix = "webapi:news:feed:uid:"


def update_user_ranking_recommend(user_id, recommend_sorted_list):
    # (user id, news id, channel id, topic (日本, 旅行)) ranking score , 30
    news_feed_json = redis_ali.get(feedSuffix + str(user_id))
    news_feed_list = []
    if news_feed_json:
        news_feed_list = json.loads(news_feed_json)

    # rtype类型:0 普通、1 热点、2 推送、3 广告、4 专题、5 图片新闻、6 视频、7 本地
    update_news_feed_list = [x for x in news_feed_list if x['rtype'] != 0]
    for item,i in zip(recommend_sorted_list, range(len(recommend_sorted_list))):
        News.format_news(item)
        item['rtype'] = i % 2
    update_news_feed_list.extend(recommend_sorted_list)

    json_str = json.dumps(recommend_sorted_list,ensure_ascii=False)
    redis_ali.set(feedSuffix + str(user_id), json_str, ex=60*30)


def predict(time_interval='10 seconds'):
    als_fm, X_and_y = als_solver.train(time_interval=time_interval)
    # recall must behind train
    users_feature_dict, users_detail_dict, users_topic_dict = \
        etl_user_data.recall_candidates(boolean_users=True, users_para=[33658617])

    for user, feature in users_feature_dict.iteritems():
        if user not in users_topic_dict:
            pass
        else:
            item_candidates, wilson_dict = etl_item_data.recall_candidates(user, users_topic_dict[user])

        if len(item_candidates) == 0:
            print '------------------wilson is empty-----'
            continue

        nt = datetime.datetime.now()
        feature.extend(etl_sample.sampleExtractor.generate_time_feature(nt))

        user_cols = [feature] * len(item_candidates)

        cols = np.hstack((user_cols, item_candidates.values()))
        feature_matrix = sp.csc_matrix(cols)

        recommend_list = als_fm.predict(feature_matrix)
        # TODO sort but keep nid...
        # should not change the item_candidates' order
        recommend_dict = dict(zip(item_candidates.keys(), recommend_list))
        sorted_list = sorted(recommend_dict.items(), key=operator.itemgetter(1), reverse=True)

        recommend_items_list = []
        for i in range(len(sorted_list)):
            recommend_items_list.append(wilson_dict[sorted_list[i][0]])
        update_user_ranking_recommend(user, recommend_items_list[:200])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Something to explain')
    parser.add_argument('--t', metavar='path', required=True,
                        help='time interval for retrieve uses')
    args = parser.parse_args()
    sleep_time = 60*60

    while True:
        st = time.time()
        predict(args.t)
        end = time.time()
        elapse = end - st
        print 'Allen Wake, you have ' + str(elapse) + ' seconds to run.'
        if elapse > sleep_time:
            print 'allen, you have a long dream. Wake up.'
            continue
        else:
            time.sleep(sleep_time - elapse)
            print 'Wake up allen, allen wake up.'