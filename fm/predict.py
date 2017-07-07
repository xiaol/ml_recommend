# -*- coding: utf-8 -*-


import als_solver
import etl_item_data
import etl_user_data
import numpy as np
import scipy.sparse as sp
from util.postgres import postgres_write_only as pg_write
import operator
from util.redis_connector import redis_ali
import json

feedSuffix = "webapi:news:feed:uid:"


def update_user_ranking_recommend(user_id, recommend_sorted_list):
    # (user id, news id, channel id, topic (日本, 旅行)) ranking score , 30
    newsFeedCache_json = redis_ali.get(feedSuffix + str(user_id))
    newsFeed_dict = json.loads(newsFeedCache_json)

    json_str = json.dumps(newsFeed_dict)
    redis_ali.set(feedSuffix + str(user_id), json_str)


if __name__ == '__main__':
    als_fm = als_solver.train()
    # recall must behind train
    users_feature_dict, users_detail_dict, users_topic_dict = \
        etl_user_data.recall_candidates(boolean_test_users=True)

    for user, feature in users_feature_dict.iteritems():
        if user not in users_topic_dict:
            pass
        else:
            item_candidates = etl_item_data.recall_candidates(users_topic_dict[user])

        user_cols = [feature] * len(item_candidates)

        cols = np.hstack((user_cols, item_candidates.values()))
        feature_matrix = sp.csc_matrix(cols)

        recommend_list = als_fm.predict(feature_matrix)
        # TODO sort but keep nid...
        # should not change the item_candidates' order
        recommend_dict = dict(zip(item_candidates.keys(), recommend_list))
        sorted_list = sorted(recommend_dict.items(), key=operator.itemgetter(1), reverse=True)

        update_user_ranking_recommend(user, sorted_list)
