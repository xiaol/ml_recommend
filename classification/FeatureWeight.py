# -*- coding: utf-8 -*-
# @Time    : 16/8/10 上午11:41
# @Author  : liulei
# @Brief   : 采用TF-IDF方法计算特征的权重
# @File    : FeatureWeight.py
# @Software: PyCharm Community Edition

import math
import os
from time import time
from DocPreProcess import DOC_NUM
from DocPreProcess import TRAIN_DOC
from DocPreProcess import category_list
from DocPreProcess import logger
from DocPreProcess import idf_file
from FeatureSelection import svm_feature_file
from FeatureSelection import textCutBasePath

TestDocCount = DOC_NUM - TRAIN_DOC #作為test的文档数目
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
train_svm_file =real_dir_path + '/../result/train.svm'
test_svm_file = real_dir_path + '/../result/test.svm'
idf_file = './result/idf.txt'

#feature_dict是为了后面的快速查询, feature_list是为了提供给训练的接口
def readFeature(featureName):
    featureFile = open(featureName, 'r')
    featureContent = featureFile.read().split('\n')
    featureFile.close()
    feature_list = list()
    feature_dict = dict()
    for eachfeature in featureContent:
        eachfeature = eachfeature.split(" ")
        if (len(eachfeature)==2):
            feature_list.append(eachfeature[1])
            feature_dict[eachfeature[1]] = eachfeature[0]
    return feature_list, feature_dict


# 读取所有类别的训练样本到字典中,每个文档是一个list
def readFileToList(textCutBasePath, classCode, documentCount):
    print 'readFileToList'
    dic = dict()
    for eachclass in classCode:
        currClassPath = textCutBasePath + eachclass + "/"
        n = len(os.listdir(currClassPath))
        n = min(n, DOC_NUM)
        eachclasslist = list()
        for i in range(n):
            eachfile = open(currClassPath+str(i)+".cut")
            eachfilecontent = eachfile.read()
            eachfilewords = eachfilecontent.split(" ")
            eachclasslist.append(eachfilewords)
        dic[eachclass] = eachclasslist
    return dic

# 读取所有类别的训练样本到字典中,每个文档是一个list
def readTestFileToList(textCutBasePath, classCode, documentCount, testDocCount):
    dic = dict()
    for eachclass in classCode:
        currClassPath = textCutBasePath + eachclass + "/"
        eachclasslist = list()
        for i in range(documentCount, documentCount+testDocCount):
            eachfile = open(currClassPath+str(i)+".cut")
            eachfilecontent = eachfile.read()
            eachfilewords = eachfilecontent.split(" ")
            eachclasslist.append(eachfilewords)
        dic[eachclass] = eachclasslist
    return dic

# 计算特征的逆文档频率
def featureIDF(dic, feature):
    print 'featureIDF'
    import FeatureSelection
    news_total_num = FeatureSelection.news_total_num
    word_cate_count = FeatureSelection.word_cate_count
    idf_dict = dict()
    print 'featureIDF  word_cate_count='+str(len(word_cate_count))
    for word in word_cate_count.keys():
        if word not in feature_list:
            continue
        n = 0
        cate_count_dict = word_cate_count[word]
        for val in cate_count_dict.values():
            n += val
        if n == 0:
            idf_dict[word] = 0
        else:
            idf_dict[word] = math.log(float(news_total_num)/float(n))
    return idf_dict


from multiprocessing import Lock, Pool
mylock = Lock()
#将tfidf写入文件
def write_tfidf_to_file(filename, s_list):
    mylock.acquire()
    file = open(filename, 'a')
    for s in s_list:
        file.write(s)
    file.close()
    mylock.release()

#计算idf进程
def cal_tfidf_proc(feature, idffeature, classid, classFiles, filename):
    #计算每个文件内特征词的tfidf
    s_list = []  #存放每个文件的tfidf 字符串
    for eachfile in classFiles:
        s = ''
        s += str(classid) + ' '
        for i in range(len(feature)):
            if feature[i] in eachfile:
                currentfeature = feature[i]
                featurecount = eachfile.count(feature[i])
                tf = float(featurecount)/(len(eachfile))
                # 计算逆文档频率
                if currentfeature in idffeature.keys():
                    featurevalue = idffeature[currentfeature]*tf
                    s += str(i+1)+":"+str(featurevalue) + " "
        s_list.append(s + '\n')
    write_tfidf_to_file(filename, s_list)


# 计算Feature's TF-IDF 值
def TFIDFCal(feature, dic, idffeature, filename):
    file = open(filename, 'w')
    file.close()
    print 'TFIDFCal --- feature num =' + str(len(feature))
    print '-------------dic num ='+ str(len(dic))
    print '-------------idf num ='+ str(len(idffeature))
    pool = Pool(30)
    for key in dic:
        classFiles = dic[key]
        classid = category_list.index(key)
        pool.apply_async(cal_tfidf_proc, (feature, idffeature, classid, classFiles, filename))
    pool.close()
    pool.join()


def writeIdfToFile(idffeature_dict):
    file0 = open(idf_file, 'w')
    for key in idffeature_dict.keys():
        file0.write(key + ' ' + str(idffeature_dict[key]) + '\n')
    file0.close()

#feature_list, feature_dict = readFeature(svm_feature_file)#读取所有的特征词
feature_list = []
feature_dict = {}

def featureWeight():
    global feature_list
    global feature_dict
    feature_list, feature_dict = readFeature(svm_feature_file)#读取所有的特征词
    logger.info('featureWeight being...')
    dic = readFileToList(textCutBasePath, category_list, TRAIN_DOC)
    idffeature = featureIDF(dic, feature_list)
    writeIdfToFile(idffeature)
    #train 数据
    TFIDFCal(feature_list, dic, idffeature, train_svm_file)
    logger.info('featureWeight done!')
    #test数据
    #test_dic = readTestFileToList(textCutBasePath, category_list, TRAIN_DOC, TestDocCount)
    #TFIDFCal(feature_list, test_dic, idffeature, test_svm_file)





