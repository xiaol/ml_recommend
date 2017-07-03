# -*- coding: utf-8 -*-


import als_solver
import etl_item_data
import etl_user_data
import numpy as np
import scipy.sparse as sp


def update_user_ranking_recommend():
    # (user id, news id, channel id, topic (日本, 旅行)) ranking score , 30
    pass


if __name__ == '__main__':
    users_feature_dict, users_detail_dict, users_topic_dict = \
        etl_user_data.recall_candidates()
    als_fm = als_solver.train()

    for user, feature in users_feature_dict.iteritems():
        if user not in users_topic_dict:
            pass
        else:
            item_candidates = etl_item_data.recall_candidates(users_topic_dict[user])

        cols = np.array([0]*10)
        feature_matrix = sp.csc_matrix(cols)

        recommend_list = als_fm.predict(feature_matrix)
        sorted_list = sorted(recommend_list, reverse=True)

        update_user_ranking_recommend()
