# -*- coding: utf-8 -*-


# references:https://www.slideshare.net/hongliangjie1/libfm

import etl_item_data
import etl_user_data
import etl_sample
import numpy as np
import scipy.sparse as sp
from memory_profiler import profile
import gc

from collections import OrderedDict


# @profile
def construct_feature_matrix(topic_num, user= [], time_interval='10 seconds'):
    """
    :param topic_num:  the number of lda topics
    :return:
    """
    if user:
        active_users = user
    else:
        active_users = etl_user_data.get_sample_user(time_interval=time_interval, click_times=5)
    print 'Users count:' + str(len(active_users))

    users_feature_dict, users_detail_dict, users_topic_dict = {}, {}, {}
    negative_samples_list, positive_samples_list, items_list = [], [], []

    # init item and user extractor
    item_extractor = etl_item_data.ItemExtractor()
    strategies_dict = item_extractor.enumerate_recommend_strategy()

    user_extractor = etl_user_data.UserExtractor()
    user_extractor.preprocess_users_feature(active_users)

    print 'step -> ',
    for splited_users in chunks(active_users, 100):

        users_feature_dict_split, users_detail_dict_split, users_topic_dict_split = user_extractor.load(
            splited_users, topic_num, '15 days')

        users_feature_dict.update(users_feature_dict_split)
        users_detail_dict.update(users_detail_dict_split)
        users_topic_dict.update(users_topic_dict_split)

        # uid nid readtime logtype logchid
        # TODO use RBM to score sample or find the negative sample
        pos_samples_list = etl_sample.get_positive_samples(splited_users, '30 days')
        short_memory_nag_samples_list = [] #  etl_sample.get_read_samples(splited_users, '1 minute')
        nag_samples_list = etl_sample.get_hate_samples(splited_users)
        nag_samples_list.extend(short_memory_nag_samples_list)

        items_list_split = [nag['nid'] for nag in nag_samples_list]
        items_list_split.extend([pos[1] for pos in pos_samples_list])
        items_list.extend(items_list_split)

        negative_samples_list.extend(nag_samples_list)
        positive_samples_list.extend(pos_samples_list)

        print str(len(items_list)) + ' ',

    items_feature_dict = etl_item_data.load(set(items_list), topic_num, item_extractor)
    print '<- Negative samples size: '+str(len(negative_samples_list)) + ' Positive samples size:' \
          + str(len(positive_samples_list)) + ' items feature size:' + str(len(items_feature_dict))

    all_samples_feature_dict = get_samples_feature(
                                negative_samples_list, users_feature_dict,
                                items_feature_dict, strategies_dict)

    features_dict = update_positive_sample_feature(
                        positive_samples_list, all_samples_feature_dict,
                        users_feature_dict, items_feature_dict,
                        strategies_dict)

    print 'features dict complete'
    feature_list = features_dict.keys()  # feature list contains tuples
    y_samples = np.array(features_dict.values())

    # import objgraph
    # objgraph.show_refs([feature_list], filename='ref_topo.png')

    del items_feature_dict
    del all_samples_feature_dict
    del features_dict
    del negative_samples_list
    del positive_samples_list
    del items_list
    del users_feature_dict_split
    del users_detail_dict_split
    del users_topic_dict_split
    del users_feature_dict
    del users_detail_dict
    del users_topic_dict
    del active_users

    cols = np.array(feature_list, copy=False)
    print 'The shape of the train cols:', cols.shape, 'user cols:', 'item cols:'
    del feature_list
    gc.collect()

    print '-> compressed to matrix:'
    feature_matrix = sp.csc_matrix(cols)
    return feature_matrix, y_samples, user_extractor, item_extractor


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def get_samples_feature(negative_samples_list,
                        users_feature_dict,
                        items_feature_dict,
                        strategies_dict):
    read_feature_dict = OrderedDict()

    light = 0
    for neg_sample in negative_samples_list:
        if neg_sample['uid'] not in users_feature_dict:
            continue  # TODO something bad happened, very very bad
        for k, v in strategies_dict.iteritems():
            strategies_dict[k] = 0

        if 'logtype' in neg_sample:
            if neg_sample['logtype'] not in strategies_dict:
                print 'Unknown logtype: ', neg_sample['logtype'],
                continue
            strategies_dict[neg_sample['logtype']] = 1

        feature_list = list(users_feature_dict[neg_sample['uid']])
        # add strategy feature
        feature_list.extend(strategies_dict.values())
        # add time feature
        if 'ctime' in neg_sample:
            feature_list.extend(etl_sample.sampleExtractor.generate_time_feature(neg_sample['ctime']))
        else:
            feature_list.extend(etl_sample.sampleExtractor.generate_time_feature(neg_sample['readtime']))

        # join the user, strategies and item features horizontally
        feature_list.extend(items_feature_dict[neg_sample['nid']])
        if light == 0:
            light = len(feature_list)
        elif light != len(feature_list):
            print '===========>  allen ,where are you?'

        feature_key = tuple(feature_list)
        read_feature_dict[feature_key] = 0

    return read_feature_dict


def update_positive_sample_feature(click_samples_list, samples_feature_dict,
                                   users_feature_dict, items_feature_dict,
                                   strategies_dict):

    light = 0
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

        if light == 0:
            light = len(feature_list)
        elif light != len(feature_list):
            print '===========>  allen ,where are you?'

        feature_key = tuple(feature_list)
        if click_sample[3] != 0:
            score = min(click_sample[3], 30)/30.0 * 5 
            samples_feature_dict[feature_key] = score
        else:
            samples_feature_dict[feature_key] = 1 

    return samples_feature_dict


if __name__ == '__main__':
    X, y, user_extractor, item_extractor = construct_feature_matrix(5000, '6 hours')
    print "hold"
