# -*- coding: utf-8 -*-
# @Time    : 16/8/8 下午3:48
# @Author  : liulei
# @Brief   : 使用卡方检验的方法提取特征词
# @File    : FeatureSelection.py.py
# @Software: PyCharm Community Edition

import gc
from DocPreProcess import *
FEATURE_NUM = 10000  #每个类别的特征词数目
# 对卡方检验所需的 a b c d 进行计算
# a：在这个分类下包含这个词的文档数量
# b：不在该分类下包含这个词的文档数量
# c：在这个分类下不包含这个词的文档数量
# d：不在该分类下，且不包含这个词的文档数量
def calChi(a, b, c, d):
    if ((a+c) * (a+b) * (b+d)*(c+d)) == 0:
        return 0;
    else:
        return float(pow((a*d - b*c), 2)) / float((a+c) * (a+b) * (b+d) * (c+d))

#分词后的文件的路径
textCutBasePath = './NewsFileCut/'
svm_feature_file = './result/SVMFeature.txt'
if not os.path.exists('./result'):
    os.mkdir('./result')
if not os.path.exists(svm_feature_file):
    f = open(svm_feature_file, 'w')
    f.close()

from multiprocessing import Process, Lock, Manager
mylock = Lock()
termDict = Manager().dict()
termClassDict = Manager().dict()
summary = Manager().dict()
#file_words_count_list : 每个文件中词语及其出现次数的字典
#words_newsnum_dict: 该类别中出现的词语及其次数
#summary : {'科技': {'count':1000, 'words':{手机:50, 小米:10} }, ....}
def collData(catetory, file_words_count_list, words_newsnum_dict):
    mylock.acquire()
    #global termClassDict
    #global termDict
    global summary
    summary[catetory] = {}
    tmp_dict = {}
    tmp_dict['count'] = len(file_words_count_list)
    tmp_dict['words'] = words_newsnum_dict
    summary[catetory] = tmp_dict
    mylock.release()

def readCategoryFileProc(category):
    print 'process id:', str(os.getpid())
    currClassPath = textCutBasePath + category + '/'
    #eachClassWordSets = set()
    #eachClassWordList = list()
    count = len(os.listdir(currClassPath))
    file_words_count_list = []  #每个文件中词在本文件中出现的次数.
    words_newsnum_dict = {}  #用于统计包含词语的文章的数量
    #from DocPreProcess import DOC_NUM
    count = min(count, DOC_NUM)
    for i in range(count):
        eachDocPath = currClassPath + str(i) + '.cut'
        eachFileObj = open(eachDocPath, 'r')
        eachFileContent = eachFileObj.read()
        eachFileWords = eachFileContent.split(' ')
        #eachFileSet = set()
        words_count_dict = {}
        for eachword in eachFileWords:
            if len(eachword) == 0:
                continue
            if eachword in words_count_dict.keys():
                words_count_dict[eachword] += 1
                continue
            else:
                words_count_dict[eachword] = 1
        for word in words_count_dict.keys():
            if word in words_newsnum_dict.keys():
                words_newsnum_dict[word] += 1
            else:
                words_newsnum_dict[word] = 1

        file_words_count_list.append(words_count_dict)
            #eachFileSet.add(eachword)
            #eachClassWordSets.add(eachword)
        #eachClassWordList.append(eachFileSet)
        eachFileObj.close()
    print 'coll' + category + 'data begin'
    logger.info('coll' + category + 'data begin')
    #collDate(catetory, eachClassWordSets, eachClassWordList)
    collData(category, file_words_count_list, words_newsnum_dict)
    print 'coll' + category + 'data end'
    logger.info('coll' + category + 'data end')


def buildItemSetsMutiProc():
    proc_name = []
    for eachclass in category_list:
        mp = Process(target=readCategoryFileProc, args=(eachclass,))
        mp.start()
        proc_name.append(mp)
    for i in proc_name:
        i.join()
    print "buildItemSets finished!"

#构建每个类别的最初词向量。set内包含所有特征词
#每个类别下的文档集合用list<set>表示,每个set表示一个文档,整体用一个dict表示
def buildItemSets():
    termDic = dict()
    termClassDic = dict()
    for eachclass in category_list:
        currClassPath = textCutBasePath + eachclass + '/'
        eachClassWordSets = set()
        eachClassWordList = list()
        count = len(os.listdir(currClassPath))
        for i in range(count):
            eachDocPath = currClassPath + str(i) + '.cut'
            eachFileObj = open(eachDocPath, 'r')
            eachFileContent = eachFileObj.read()
            eachFileWords = eachFileContent.split(' ')
            eachFileSet = set()
            for eachword in eachFileWords:
                if len(eachword) == 0:
                    continue
                eachFileSet.add(eachword)
                eachClassWordSets.add(eachword)
            eachClassWordList.append(eachFileSet)
            eachFileObj.close()
            del eachFileContent
            del eachFileWords
            gc.collect()
        termDic[eachclass] = eachClassWordSets
        termClassDic[eachclass] = eachClassWordList
        del eachClassWordList
        del eachClassWordSets
        gc.collect()

    print "buildItemSets finished!"
    return termDic, termClassDic

word_cate_count = {} #用于统计一个词语在不同分类中出现的次数
news_total_num = 0 #文章总数
#收集特征
mylock2 = Lock()
cate_selected_dict = Manager().dict()
def coll_feature(category, subDic):
    mylock2.acquire()
    global cate_selected_dict
    cate_selected_dict[category] = subDic
    mylock2.release()

#featureSelection子进程
def feature_select_proc(K, data, category, word_cate_count, news_total_num):
    word_chi = {}
    count = data['count']
    words = data['words']
    for word in words.keys():
        a = words[word] #该类下改词出现的次数
        b = 0 #不在该类下,该词出现的次数
        cate_num_dict = word_cate_count[word]
        for cate in cate_num_dict.keys():
            if cate != category:
                b += cate_num_dict[cate]
        c = count - a #该类下不包含该词的数目
        d = news_total_num - count - b #不在该类,且不包含该词
        word_chi[word] = calChi(a, b, c, d)
    #排序后取前K个
    #排序后返回的是元组的列表
    sorted_word_chi = sorted(word_chi.items(), key=lambda d:d[1], reverse=True)
    subDic = dict()
    n = min(len(sorted_word_chi), K)
    for i in range(n):
        subDic[sorted_word_chi[i][0]] = sorted_word_chi[i][1]
    print '----' + str(len(subDic))
    coll_feature(category, subDic)

def featureSelection2(K):
    print 'featureSelection(K) begin...'
    global summary
    global word_cate_count
    global news_total_num
    global cate_selected_dict
    for category in summary.keys():
        words_dict = summary[category]['words']
        for word in words_dict.keys():
            if word not in word_cate_count.keys():
                word_cate_count[word] = {}
                word_cate_count[word][category] = words_dict[word]
            else:
                word_cate_count[word][category] = words_dict[word]

    for cate in summary.keys():
        news_total_num += summary[cate]['count']

    from multiprocessing import Pool
    pool = Pool(30)
    for category in summary.keys():
        data = summary[category]
        pool.apply_async(feature_select_proc, (K, data, category, word_cate_count, news_total_num))
    pool.close()
    pool.join()

    return cate_selected_dict

#计算卡方,选取特征词
#K为每个类别的特征词数目
def featureSelection(termDic, termClassDic, K):
    logger.info('featureSelect()...')
    print 'featureSelect() begin...'
    termCountDic = dict()
    for key in termDic.keys():
        #classWordSets = termDic[key]
        classTermCountDic = dict()
        for eachword in termDic[key]:
            a = 0
            b = 0
            c = 0
            d = 0
            for eachclass in termClassDic.keys():
                if eachclass == key: #在这个类别下处理
                    for eachdocset in termClassDic[eachclass]:
                        if eachword in eachdocset:
                            a += 1
                        else:
                            c += 1
                else: #类别外处理
                    #print len(termClassDic[eachclass])
                    for eachdocset in termClassDic[eachclass]:
                        if eachword in eachdocset:
                            b += 1
                        else:
                            d += 1
            #eachwordcount = calChi(a, b, c, d)
            classTermCountDic[eachword] = calChi(a, b, c, d)
        #排序后取前K个
        #排序后返回的是元组的列表
        sortedClassTermCountDic = sorted(classTermCountDic.items(), key=lambda d:d[1], reverse=True)
        subDic = dict()
        n = min(len(sortedClassTermCountDic), K)
        for i in range(n):
            subDic[sortedClassTermCountDic[i][0]] = sortedClassTermCountDic[i][1]
        termCountDic[key] = subDic
    logger.info('featureSelect() finished...')
    print 'featureSelect() end...'
    return termCountDic

def writeFeatureToFile(termCounDic, fileName):
    logger.info('write features to file ...')
    featureSet = set()
    for key in termCounDic.keys():
        for eachkey in termCounDic[key]:
            featureSet.add(eachkey)
    count = 1
    file = open(fileName, 'w')
    for feature in featureSet:
        #判断feature不为空
        stripfeature = feature.strip(' ')
        if len(stripfeature) > 0 and feature != ' ' and (not feature.isspace()) and (not feature.isdigit()):
            file.write(str(count) + ' ' + feature + '\n')
            count += 1
    file.close()
    logger.info('write features to file finished!')

def featureSelect():
    global termDict
    global termClassDict
    logger.info('featureSelect begin...')
    #termDic, termClassDic = buildItemSets()
    buildItemSetsMutiProc()
    #termCountDic = featureSelection(termDict, termClassDict, FEATURE_NUM)
    termCountDic = featureSelection2(FEATURE_NUM)
    writeFeatureToFile(termCountDic, svm_feature_file)
    logger.info('featureSelect done!')

#调用buildItemSets,创造train数据
#每个类别取前200个文件; 每个类别取1000个特征词
#termDic, termClassDic = buildItemSets(200)
#termCountDic = featureSelection(termDic, termClassDic, 1000)
#writeFeatureToFile(termCountDic, 'SVMFeature.txt')



