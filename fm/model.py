# -*- coding: utf-8 -*-


# references:https://www.slideshare.net/hongliangjie1/libfm

import etl_item_data
import etl_user_data
import numpy as np
import scipy.sparse as sp


def construct_feature_matrix():
    user_feature_dict = etl_user_data.load()
    item_feature_dict = etl_item_data.load()

    # uid 1, item id 1
    user_cols = np.array(user_feature_dict.values())
    item_cols = np.array(item_feature_dict.values())

    cols = np.hstack((user_cols, item_cols))

    feature_matrix = sp.csc_matrix(cols)
    ratings_vector = np.array([1, 0, 1, 0])
    return feature_matrix, ratings_vector


if __name__ == '__main__':
    X, y = construct_feature_matrix()
    print "hold"
