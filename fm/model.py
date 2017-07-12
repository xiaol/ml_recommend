# -*- coding: utf-8 -*-


# references:https://www.slideshare.net/hongliangjie1/libfm

import etl_item_data
import etl_user_data
import etl_sample
import numpy as np
import scipy.sparse as sp
from memory_profiler import profile
import gc


@profile
def construct_feature_matrix(topic_num, time_interval='10 seconds'):
    """
    :param topic_num:  the number of lda topics
    :return:
    """
    active_users = etl_user_data.get_active_user(time_interval=time_interval)
    etl_user_data.user_extractor.preprocess_users_feature(active_users)
    print 'Users count:' + str(len(active_users))

    users_feature_dict, users_detail_dict, users_topic_dict = {}, {}, {}
    read_samples_list, click_samples_list, items_list = [], [], []

    print 'step -> ',
    for splited_users in chunks(active_users, 100):
        users_feature_dict_split, users_detail_dict_split, users_topic_dict_split = etl_user_data.\
            user_extractor.load(splited_users, topic_num, '15 days')
        users_feature_dict.update(users_feature_dict_split); users_detail_dict.update(users_detail_dict_split); users_topic_dict.update(users_topic_dict_split)

        # uid nid readtime logtype logchid
        read_samples_list_split = etl_sample.get_read_samples(splited_users, '45 minutes')
        click_samples_list_split = etl_sample.get_click_samples(splited_users, '12 hours')
        read_samples_list.extend(read_samples_list_split); click_samples_list.extend(click_samples_list_split)

        items_list_split = [read_sample[1] for read_sample in read_samples_list]
        items_list_split.extend([click_sample[1] for click_sample in click_samples_list])
        items_list.extend(items_list_split)
        print str(len(items_list)) + ' ',

    items_feature_dict = etl_item_data.load(set(items_list), topic_num)
    print '<- Read samples size: '+str(len(read_samples_list)) + ' click samples size:' + str(len(click_samples_list)) + ' items feature size:' + str(len(items_feature_dict))

    read_samples_feature_dict = get_samples_feature(
                                read_samples_list, users_feature_dict, items_feature_dict)
    features_dict = get_positive_sample_feature(
                        click_samples_list, read_samples_feature_dict,
                        users_feature_dict, items_feature_dict)

    print 'features dict complete'
    feature_list = features_dict.keys()  # feature list contains tuples
    y_samples = np.array(features_dict.values())

    # import objgraph
    # objgraph.show_refs([feature_list], filename='ref_topo.png')

    del items_feature_dict
    del read_samples_feature_dict
    del features_dict
    del read_samples_list
    del click_samples_list
    del items_list
    del users_feature_dict_split
    del users_detail_dict_split
    del users_topic_dict_split
    del users_feature_dict
    del users_detail_dict
    del users_topic_dict
    del active_users

    cols = np.array(feature_list, copy=False)
    del feature_list
    gc.collect()

    print '-> compressed to matrix'
    feature_matrix = sp.csc_matrix(cols)
    return feature_matrix, y_samples


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def get_samples_feature(read_samples_list, users_feature_dict, items_feature_dict):
    strategies_dict = etl_item_data.enumerate_recommend_strategy()
    read_feature_dict = {}

    for read_sample in read_samples_list:
        if read_sample[0] not in users_feature_dict:
            continue  # TODO something bad happened, very very bad
        for k, v in strategies_dict.iteritems():
            strategies_dict[k] = 0

        if read_sample[3] not in strategies_dict:
            print 'Unknown logtype: ', read_sample[3],
            continue
        strategies_dict[read_sample[3]] = 1

        feature_list = list(users_feature_dict[read_sample[0]])
        feature_list.extend(strategies_dict.values())

        feature_list.extend(etl_sample.sampleExtractor.generate_time_feature(read_sample[2]))

        # join the user, strategies and item features horizontally
        feature_list.extend(items_feature_dict[read_sample[1]])

        feature_key = tuple(feature_list)
        read_feature_dict[feature_key] = 0

    return read_feature_dict


def get_positive_sample_feature(click_samples_list, samples_feature_dict,
                                users_feature_dict, items_feature_dict):
    strategies_dict = etl_item_data.enumerate_recommend_strategy()

    for click_sample in click_samples_list:
        if click_sample[0] not in users_feature_dict:
            continue  # TODO something bad happened, very very bad
        for k, v in strategies_dict.iteritems():
            strategies_dict[k] = 0

        if click_sample[4] not in strategies_dict:
            print 'Unknown logtype ', click_sample[4],
            continue
        strategies_dict[click_sample[4]] = 1

        feature_list = list(users_feature_dict[click_sample[0]])
        feature_list.extend(strategies_dict.values())

        feature_list.extend(items_feature_dict[click_sample[1]])

        feature_list.extend(etl_sample.sampleExtractor.generate_time_feature(click_sample[2]))

        feature_key = tuple(feature_list)
        samples_feature_dict[feature_key] = 1

    return  samples_feature_dict


if __name__ == '__main__':
    X, y = construct_feature_matrix(5000, '10 seconds')
    print "hold"
