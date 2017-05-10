# -*- coding: utf-8 -*-
# @Time    : 16/8/10 下午2:33
# @Author  : liulei
# @Brief   : 
# @File    : SVMClassify.py
# @Software: PyCharm Community Edition
import os
import datetime, calendar
import traceback

import re
import string

import sys
from sklearn.datasets import load_svmlight_file
from sklearn.grid_search import GridSearchCV
#from sklearn.model_selection import GridSearchCV
from sklearn.svm import SVC
from classification.DocPreProcess import strProcess
from classification.DocPreProcess import getTextOfNewsNids
from classification.DocPreProcess import category_list
from classification.DocPreProcess import category_name_id_dict
from classification.DocPreProcess import logger
from classification.FeatureWeight import train_svm_file
from classification.FeatureWeight import idf_file
param_grid = {'C': [10, 100, 1000], 'gamma': [0.001, 0.0001]}
clf = GridSearchCV(SVC(kernel='rbf'), param_grid)

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
svm_file = real_dir_path + '/../result/svm_predict.txt'
model_file =real_dir_path +  '/../result/svm.model'

#多进程时需要多个写出文件
def set_news_predict_svm_file(port):
    global svm_file
    svm_file = real_dir_path + '/../result/svm_predict_' + str(port) + '.txt'

from sklearn.externals import joblib
n_feature = 0
def getModel():
    print 'getModel() begin...'
    global clf
    global n_feature
    if os.path.exists(model_file):
        #获取特征数量
        X_train, y_train = load_svmlight_file(train_svm_file)
        n_feature = X_train.shape[1]

        clf = joblib.load(model_file)
        logger.info('load model successfully!')
    else:
        logger.info('Begin to train...')
        X_train, y_train = load_svmlight_file(train_svm_file)
        n_feature = X_train.shape[1]
        start_time = datetime.datetime.now()
        clf.fit(X_train, y_train)
        end_time = datetime.datetime.now()
        joblib.dump(clf, model_file)
        print 'getModel finished!'
        logger.info('train done in {0}s'.format((end_time - start_time).total_seconds()))
    #X_test, y_test = load_svmlight_file('test.svm', n_features=X_train.shape[1])#没有指定n_features时出现X_test.shape[1] != X_train.shape[1]
    #pred = clf.predict(X_test)
    #from sklearn.metrics import accuracy_score
    #print accuracy_score(y_test, pred)

idf_val = {}
def getIdfOfTrain():
    global idf_val
    if not os.path.exists(idf_file):
        logger.error('no idf.txt')
        return
    else:
        f = open(idf_file, 'r')
        lines = f.readlines()
        for line in lines:
            line2 = re.split(' ', line)
            idf_val[line2[0]] = string.atof(line2[1])
        f.close()

def writeSvmFile(text, file_path, idf_val):
    from classification.FeatureWeight import feature_dict
    if len(feature_dict) == 0:
        from classification.FeatureWeight import readFeature
        from classification.FeatureSelection import svm_feature_file
        feature_list, feature_dict = readFeature(svm_feature_file)
    try:
        svm_file = open(file_path, 'a')
        words = strProcess(text)
        svm_file.write(str(-1) + ' ')
        checked = []
        data = dict()
        for w in words:
            w_utf = w.encode('utf-8')
            if (w_utf in feature_dict.keys()) and (w_utf in idf_val.keys()) and (w_utf not in checked):
                checked.append(w_utf)
                feature_count = words.count(w)
                currTf = float(feature_count)/len(words)
                val = idf_val[w_utf]*currTf
                data[int(feature_dict[w_utf])] = str(val)
        data_list = sorted(data.iteritems(), key=lambda d:d[0])
        for item in data_list:
            svm_file.write(str(item[0]) + ':' + item[1] + ' ')
        svm_file.write('\n')
        svm_file.close()
    except IOError as e:
        logger.error('I/O error({0}):{1}'.format(e.errno, e.strerror))
    except Exception, e:
        logger.error(traceback.format_exc())
        logger.error('Unexpected error when writeSvmFile:{0}'.format(sys.exc_info()[0]))

#预测单个文本
#@tornado.gen.coroutine
def svmPredictOneText(text):
    global idf_val
    if text =='' or text.isspace():
        return {'res': False, 'category': ''}
    predict_file = open(svm_file, 'w')
    predict_file.close()
    if len(idf_val) == 0:
        getIdfOfTrain()
    writeSvmFile(text, svm_file, idf_val)
    X_pre, y_pre = load_svmlight_file(svm_file, n_features=n_feature)
    pred = clf.predict(X_pre)
    if pred and int(pred[0]):
        return {'bSuccess': True, 'category': category_list[int(pred[0])]}
    else:
        return {'bSuccess': False, 'category': ''}
#预测多个文本
def svmPredictTexts(texts):
    global idf_val
    predict_file = open(svm_file, 'w')
    predict_file.close()
    if len(idf_val) == 0:
        getIdfOfTrain()
    for text in texts:
        writeSvmFile(text, svm_file, idf_val)
    logger.info('write svm file to predict done!')
    X_pre, y_pre = load_svmlight_file(svm_file, n_features=n_feature)
    pred = clf.predict(X_pre)
    return pred

sql = 'SELECT a.nid, a.title, a.content, b.cname  \
FROM \
(SELECT * from newslist_v2 where srid={0}) a \
INNER JOIN channellist_v2 b \
on \
a.chid = b.id '
#根据srcid从数据库中去数据进行预测
#@tornado.gen.coroutine
def svmPredictNews(nids, texts, _id = 0, category='all'):
    start_time = datetime.datetime.now()
    logger.info('svmPredictOnSrcid begin...')
    if not category:
        category = 'all'
    if category and category != 'all' and (category not in category_list):
        logger.error('unkown category:{0}'.format(category))
        return {'bSuccess':False, 'message':'{0} is not a known category.'.format(category)}
    pred = svmPredictTexts(texts)
    today = str(datetime.datetime.now())[0:10]
    time = str(datetime.datetime.now())[11:19]
    result_file_name = './result/' + today + '.' + time + '.' + str(_id)
    logger.info('result file is ' + result_file_name)
    result_file = open(result_file_name, 'w')
    cates_dict = {}
    for i in range(len(category_list)):
        cates_dict[category_list[i]] = []
    for i in range(len(texts)):
        result_file.write(category_list[int(pred[i])] + '-----' + texts[i] + '\n')
        cates_dict[category_list[int(pred[i])]].append(nids[i])
    result_file.close()
    end_time = datetime.datetime.now()
    t = (end_time - start_time).total_seconds()
    logger.info('---predict news of srcid:{0} done, in {1}s!--------'.format(str(_id), t))
    if category == 'all':
        return {'bSuccess': True, 'nids': cates_dict}
    else:
        return {'bSuccess': True, 'nids': cates_dict[category]}


#返回格式不同
def svmPredictNews2(nids):
    logger.info('svmPredictOnNids begin...')
    texts = getTextOfNewsNids(nids)
    if len(texts) == 0:
        return {'bSuccess': False, 'result': 'nothing done!'}
    pred = svmPredictTexts(texts)
    nid_cate_list = []
    for i in range(len(texts)):
        nid_cate_list.append({'chid': category_name_id_dict[category_list[int(pred[i])]], "nid": int(nids[i])})
    logger.info('---predict news done!--------')
    return {'bSuccess': True, 'result': nid_cate_list}

