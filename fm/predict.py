# -*- coding: utf-8 -*-


import als_solver
import etl_item_data
import etl_user_data
import numpy as np
import scipy.sparse as sp
from util.postgres import postgres_write_only as pg_write


def update_user_ranking_recommend():
    # (user id, news id, channel id, topic (日本, 旅行)) ranking score , 30
    sql_delete = '''
        DELETE FROM news_ranking_users_fm
    '''
    pg_write.query(sql_delete)


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

        strategies_dict = etl_item_data.enumerate_recommend_strategy()
        feature.extend(strategies_dict.values())
        user_cols = [feature] * len(item_candidates)

        cols = np.hstack((user_cols, item_candidates.values()))
        feature_matrix = sp.csc_matrix(cols)

        recommend_list = als_fm.predict(feature_matrix)
        # TODO sort but keep nid...
        sorted_list = sorted(recommend_list, reverse=True)

        update_user_ranking_recommend()
