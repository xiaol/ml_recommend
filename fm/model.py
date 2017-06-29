# -*- coding: utf-8 -*-


# references:https://www.slideshare.net/hongliangjie1/libfm

import etl_item_data
import etl_user_data
import etl_sample
import numpy as np
import scipy.sparse as sp


def construct_feature_matrix():
    active_users = etl_user_data.get_active_user(time_interval='5 minutes')

    users_feature_dict = etl_user_data.load(active_users)

    # uid nid readtime logtype logchid
    read_samples_list = etl_sample.get_read_samples(active_users)
    click_samples_list = etl_sample.get_click_samples(active_users)

    read_sample_feature_dict = \
        get_read_sample_feature(read_samples_list, users_feature_dict)
    feature_dict = get_click_sample_feature(
        click_samples_list, read_sample_feature_dict, users_feature_dict)

    cols = np.array([list(t) for t in feature_dict.keys()])
    feature_matrix = sp.csc_matrix(cols)
    y_samples = np.array(feature_dict.values())
    return feature_matrix, y_samples


def get_read_sample_feature(read_samples_list, users_feature_dict):
    strategies_dict = etl_item_data.enumerate_recommend_strategy()
    read_feature_dict = {}
    for read_sample in read_samples_list:
        if read_sample[0] not in users_feature_dict:
            continue  # TODO something bad happened, very very bad
        copy_strategies_dict = strategies_dict.copy()
        copy_strategies_dict[read_sample[3]] = 1

        copy_user_feature_list = list(users_feature_dict[read_sample[0]])
        copy_user_feature_list.extend(copy_strategies_dict.values())
        feature_key = tuple(copy_user_feature_list)
        read_feature_dict[feature_key] = 0

    return read_feature_dict


def get_click_sample_feature(click_samples_list, read_samples_feature, users_feature_dict):
    strategies_dict = etl_item_data.enumerate_recommend_strategy()
    for click_sample in click_samples_list:
        if click_sample[0] not in users_feature_dict:
            continue  # TODO something bad happened, very very bad
        copy_strategies_dict = strategies_dict.copy()
        copy_strategies_dict[click_sample[4]] = 1

        copy_user_feature_list = list(users_feature_dict[click_sample[0]])
        copy_user_feature_list.extend(copy_strategies_dict.values())
        feature_key = tuple(copy_user_feature_list)
        read_samples_feature[feature_key] = 1

    return read_samples_feature


if __name__ == '__main__':
    X, y = construct_feature_matrix()
    print "hold"
