# -*- coding: utf-8 -*-


# references:https://www.slideshare.net/hongliangjie1/libfm

import etl_item_data
import etl_user_data
import etl_sample
import numpy as np
import scipy.sparse as sp


def construct_feature_matrix(topic_num):
    """
    :param topic_num:  the number of lda topics
    :return:
    """
    active_users = etl_user_data.get_active_user(time_interval='2 minutes')

    users_feature_dict = etl_user_data.load(active_users, topic_num, '15 days')

    # uid nid readtime logtype logchid
    read_samples_list = etl_sample.get_read_samples(active_users, '2 days')
    click_samples_list = etl_sample.get_click_samples(active_users, '2 days')

    items_list = [read_sample[1] for read_sample in read_samples_list]
    items_list.extend([click_sample[1] for click_sample in click_samples_list])

    items_feature_dict = etl_item_data.load(set(items_list), topic_num)

    read_sample_feature_dict = get_read_sample_feature(
                                read_samples_list, users_feature_dict, items_feature_dict)
    feature_dict = get_click_sample_feature(
                        click_samples_list, read_sample_feature_dict,
                        users_feature_dict, items_feature_dict)

    cols = np.array([list(t) for t in feature_dict.keys()])
    feature_matrix = sp.csc_matrix(cols)
    y_samples = np.array(feature_dict.values())

    return feature_matrix, y_samples


def get_read_sample_feature(read_samples_list, users_feature_dict, items_feature_dict):
    strategies_dict = etl_item_data.enumerate_recommend_strategy()
    read_feature_dict = {}

    for read_sample in read_samples_list:
        if read_sample[0] not in users_feature_dict:
            continue  # TODO something bad happened, very very bad
        copy_strategies_dict = strategies_dict.copy()
        copy_strategies_dict[read_sample[3]] = 1

        feature_list = list(users_feature_dict[read_sample[0]])
        feature_list.extend(copy_strategies_dict.values())

        feature_list.extend(items_feature_dict[read_sample[1]])

        feature_key = tuple(feature_list)
        read_feature_dict[feature_key] = 0

    return read_feature_dict


def get_click_sample_feature(click_samples_list, read_samples_feature,
                             users_feature_dict, items_feature_dict):
    strategies_dict = etl_item_data.enumerate_recommend_strategy()

    for click_sample in click_samples_list:
        if click_sample[0] not in users_feature_dict:
            continue  # TODO something bad happened, very very bad
        copy_strategies_dict = strategies_dict.copy()
        copy_strategies_dict[click_sample[4]] = 1

        feature_list = list(users_feature_dict[click_sample[0]])
        feature_list.extend(copy_strategies_dict.values())

        feature_list.extend(items_feature_dict[click_sample[1]])

        feature_key = tuple(feature_list)
        read_samples_feature[feature_key] = 1

    return read_samples_feature


if __name__ == '__main__':
    X, y = construct_feature_matrix(5000)
    print "hold"
