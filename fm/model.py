# -*- coding: utf-8 -*-


# references:https://www.slideshare.net/hongliangjie1/libfm

import etl_item_data
import etl_user_data
import etl_sample
import numpy as np
import scipy.sparse as sp


def construct_feature_matrix():
    active_users = etl_user_data.get_active_user(time_interval='30 minutes')

    user_feature_dict = etl_user_data.load(active_users)

    # uid nid readtime logtype logchid
    read_samples_list = etl_sample.get_read_samples(active_users)
    click_list = etl_sample.get_click_samples(active_users)

    items_list = []
    item_feature_dict = etl_item_data.load(items_list)

    y_sample = []
    # uid 1, item id 1
    user_cols = np.array(user_feature_dict.values())
    item_cols = np.array(item_feature_dict.values())
    cols = np.hstack((user_cols, item_cols))

    for read_sample in read_samples_list:  # TODO need sort
        y_sample.append(0)

        item_col = item_feature_dict[read_sample[1]]
        user_col = user_feature_dict[read_sample[0]]
        model_col = user_col.append(item_col)

        cols.append(model_col)

    feature_matrix = sp.csc_matrix(cols)
    ratings_vector = np.array([1, 0, 1, 0])
    return feature_matrix, ratings_vector


if __name__ == '__main__':
    X, y = construct_feature_matrix()
    print "hold"
