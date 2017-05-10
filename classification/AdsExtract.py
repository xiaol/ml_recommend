# -*- coding: utf-8 -*-
# @Time    : 16/10/9 下午2:41
# @Author  : liulei
# @Brief   : 
# @File    : AdsExtract.py.py
# @Software: PyCharm Community Edition
import json
import os

import jieba
#定义两个句子内容相同
def is_sentenses_same(s1, s2):
    if len(s1) > 1.5 * len(s2) or len(s2) > 1.5* len(s1):
        return False
    s1_words = jieba.cut(s1)
    s2_words = jieba.cut(s2)
    set1 = set()
    set2 = set()
    for w in s1_words:
        set1.add(w)
    for w in s2_words:
        set2.add(w)
    l1 = len(set1)
    l2 = len(set2)
    n = len(set1 & set2)
    if float(n) > float(l1 + l2)/2 * 0.8:
        return True
    else:
        return False

#求两篇新闻相同的段落
def get_same_paras(paras1, paras2):
    same_para_info = {}
    nid1 = paras1[0]
    nid2 = paras2[0]
    N1 = len(paras1) - 1
    N2 = len(paras2) - 1
    n1 = 0
    same_num = 0
    paras1_p = paras1[1:]
    paras2_p = paras2[1:]
    for p1 in paras1_p:
        n2 = 0
        bP1Finished = False
        p1_len = len(p1)
        if p1_len == 0:
            n1 += 1
            continue
        for p2 in paras2_p:
            if len(p2)==0 or (p1_len > 1.5 * len(p2) or len(p2) > 1.5* p1_len) or ((p1[0] not in p2) and (p2[0] not in p1)):
                n2 += 1
                continue
            if is_sentenses_same(p1, p2):
                if n2 < 5 and n1 < 5:
                    if n2 == n1:
                        same_para_info[str(n1)] = (p1, nid1)
                    else:
                        same_para_info[str(n1)] = (p1, nid1)
                        same_para_info[str(n2)] = (p2, nid2)
                elif n1 - N1 == n2 - N2: #位于文章结尾
                    same_para_info[str(n1 - N1)] = (p1, nid1)
                else:
                    same_para_info[str(n1 - N1)] = (p1, nid1)
                    same_para_info[str(n2 - N2)] = (p2, nid2)
                if n1 - N1 == 0:
                    print 'n1 === N1 ==== ' + p1
                if n2 - N2 == 0:
                    print 'n2 === N2 ==== '+ p2
                same_num += 1
                bP1Finished = True
            n2 += 1
            if bP1Finished:  #如果P1已经与p2匹配,则不再比较p1与p2后面的句子
                break
        n1 += 1
    #重复度<90%才认为是不同的文章,否则认定为内容相同
    #if float(same_num) < float(N1 + N2)/2 * 0.9:
    if (float(same_num) < float(N1) * 0.9) and (float(same_num) < float(N2) * 0.9):
        return False, same_para_info    #不是同一篇文章,返回相同的段落
    else:
        return True, dict()   #相同文章

#求两篇新闻相同的段落, 严格从开头和结尾逐段查找
def get_same_paras2(paras1, paras2):
    same_para_info = {}
    N1 = len(paras1)
    N2 = len(paras2)
    n1 = 0
    same_num = 0
    #判断开头是否一样
    for i in xrange(min(N1, N2)):
        if is_sentenses_same(paras1[i], paras2[i]):
            same_para_info[str(i)] = paras1[i]
            same_num += 1
        else:
            break
    #判断结尾是否一直
    for i in xrange(-1, 0-min(N1, N2), -1):
        if is_sentenses_same(paras1[i], paras2[i]):
            same_para_info[str(i)] = paras1[i]
            same_num += 1
        else:
            break
    #重复度<90%才认为是不同的文章,否则认定为内容相同
    #if float(same_num) < float(N1 + N2)/2 * 0.9:
    if (float(same_num) < float(N1) * 0.9) and (float(same_num) < float(N2) * 0.9):
        return False, same_para_info    #不是同一篇文章,返回相同的段落
    else:
        return True, dict()   #相同文章

#结果汇总
from multiprocessing import Process, Lock, Manager, Pool
mylock = Lock()
out_dict = Manager().dict()
def coll_result(key, val):
    global out_dict
    mylock.acquire()
    out_dict[key] = val
    mylock.release()


#根据公众号的文章提取公众号的广告, 子进程
#news_dict 格式: {'果壳':[段落1, 段落2,...],
#                      [段落1, 段落2,...]...,
#                 '美国咖': ...... }
#output: {'果壳':[(0, 段落0的内容), (-1, 段落-1的内容)...],
#         '美国咖'...... }
def extract_ads_proc(name, news):
    print 'pid = ', str(os.getpid())
    num = len(news)
    i = 0
    ads_dict = {}
    ads_nid_dict = {}
    while i < len(news):
        k = i + 1
        while k < len(news):
            #bSameNews, same_dict = get_same_paras2(news[i], news[k])
            bSameNews, same_dict = get_same_paras(news[i], news[k])
            if bSameNews:
                del news[k]
                continue
            for item in same_dict.items():
                el = item[0] + '\t|' + item[1][0] #item[1][0]是段落字符串, item[1][0]是nid
                if el not in ads_dict:
                    ads_dict[el] = 1
                    ads_nid_dict[el] = []
                    ads_nid_dict[el].append(item[1][1])
                else:
                    ads_dict[el] += 1
                    if item[1][1] not in ads_nid_dict[el] and len(ads_nid_dict[el]) < 4:
                        ads_nid_dict[el].append(item[1][1])
            k += 1
        i += 1
    sorted_dict = sorted(ads_dict.items(), key=lambda d:d[1], reverse=True)

    tmp_list = []
    for i in sorted_dict:
        #if float(i[1]) > float(num/3):
        if float(i[1]) > float((num * (num -1))/10):
            para = i[0].split('\t|')
            tmp_list.append( (int(para[0]), para[1], ads_nid_dict[i[0]]) )
    sorted_list = sorted(tmp_list, key=lambda d:d[0])
    ads_list = []
    for para in sorted_list:
        ads_list.append( (int(para[0]), para[1], para[2], 1)) #第四个元素1表示checked。当页面上去选时,置为0

    coll_result(name, ads_list)

real_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
ads_data_file = real_path + '/../result/ads_data.txt'
ads_update_data_file = real_path + '/../result/ads_update_data_.txt'
def extract_ads(news_dict):
    global out_dict
    pool = Pool(30)
    for item in news_dict.items():
        pool.apply_async(extract_ads_proc, (item[0], item[1]))
    pool.close()
    pool.join()
    #将结果由Manager.dict换到普通dict
    ret = {}
    for item in out_dict.items():
        ret[item[0]] = item[1]
    if not os.path.exists(ads_data_file):
        f = open(ads_data_file, 'w')
        f.write(json.dumps(ret))
        f.close()
        read_data()
    else:
        f = open(ads_update_data_file, 'w')
        f.write(json.dumps(ret))
        f.close()
        read_data()
        read_update_data()
        merge_data()
    print '***********************extract finished!'
    return

ads_dict = {}
#读取广告数据
def read_data():
    global ads_dict
    with open(ads_data_file, 'r') as f:
        r = f.read()
        ads_dict = json.loads(r)
#读取最新的广告数据
ads_update_dict = {}
def read_update_data():
    global ads_update_dict
    with open(ads_update_data_file, 'r') as f:
        r = f.read()
        ads_update_dict = json.loads(r)

#广告变更
modify_dict = set()
#整合最新的广告数据和以前的数据, 主要是获取就数据中已经被手动去除的段落
def merge_data():
    global ads_dict
    global ads_update_dict
    global modify_dict
    modify_dict.clear()
    #检查每一个公众号
    for item in ads_update_dict.items():
        modifed = False
        if item[0] not in ads_dict.keys():
            print 'add =--- ' + item[0]
            modify_dict.add(item[0])
            modifed = True
        else:
            #判断流程:先判断是否有段落上的变化,有的话添加到modify_dict; 另外需要更新最新数据中的check状态
            update_para_list = item[1]
            old_para_list = ads_dict[item[0]]
            s1 = set() #用于保存最新的广告段落
            s2 = set() #用于保存久的广告段落
            if len(update_para_list) != len(old_para_list):
                modifed = True
            for update_elem in update_para_list:
                if not modifed:
                    s1.add(str(update_elem[0]) + update_elem[1])
                for old_elem in old_para_list:
                    if not modifed:
                        s2.add(str(old_elem[0]) + old_elem[1])
                    if old_elem[3] != 0 or update_elem[0] != old_elem[0] or update_elem[1] != old_elem[1]:
                        continue
                    update_elem[3] = 0
            if modifed == False:
                if len(s1 - s2) != 0:
                    modifed = True
                    modify_dict.add(item[0])
                    print 'lens1 - s2----' + item[0]
    #替换原先的文件
    ads_dict = ads_update_dict
    save_ads_modify()


def get_ads_of_one_wechat(name):
    global ads_dict
    if len(ads_dict) == 0:
        read_data()
    return ads_dict[name]

#为基于nid获取广告提供的方法
def get_ads_paras(pname, content_list):
    global ads_dict
    if len(ads_dict) == 0:
        read_data()
    if pname not in ads_dict:
        return
    ads_paras = ads_dict[pname]
    i = 0
    to_remove = []
    while i < len(ads_paras):
        if ads_paras[i][0] < 0: #结尾有广告
            para = ads_paras[i]
            n = int(para[0])
            if len(content_list) < abs(n):
                i += 1
                continue
            content = content_list[n]
            if is_sentenses_same(content, para[1]):
                to_remove.append(n)
                if i + 1 >= len(ads_paras):
                    break
                for k in xrange(i + 1, len(ads_paras)):
                    if ads_paras[k][0] < 0:
                        i = k +1
                        continue
                    else:
                        i = k
                        break
            else:
                i += 1
                continue
        else: #新闻开头有广告
            k = len(ads_paras) - 1
            while k >= i:
                para = ads_paras[k]
                n = int(para[0])
                if len(content_list) < abs(n):
                    k -= 1
                    continue
                content = content_list[n]
                if is_sentenses_same(content, para[1]):
                    to_remove.append(n)
                    break
                else:
                    k -= 1
            break

    result = {}
    for i in to_remove:
        if i < 0:
            result.update({str(i):content_list[i]})
        else:
            result.update({str(i):content_list[i]})
    print '!Get ads done'
    return result

#modify_type: 操作类型,指添加还是删除
#modify_data: 操作数据. 结构:   微信号名称:段落号:段落内容
def modify_ads_results(modify_type, modify_data):
    global ads_dict
    data = modify_data.split(':')
    name = data[0]
    if len(data) < 3 or name not in ads_dict.keys():
        return False, 'error input data'
    p_num = data[1]
    p_text = data[2]
    print 'name=', name
    print 'pnum=', str(p_num)
    print 'p_text', p_text
    paras = ads_dict[name.decode("utf-8")]
    if modify_type == 'delete':
        for item in paras:
            para = ''.join(item[1].split())
            para2 = ''.join(p_text.decode('utf-8').split())
            if item[0] == int(p_num) and para == para2:
                print 'delete'
                item[3] = 0
                break
    elif modify_type == 'add':
        for item in paras:
            para = ''.join(item[1].split())
            if item[0] == int(p_num) and para == ''.join(p_text.encode('utf-8').split()):
                print 'add'
                item[3] = 1
                break

    return True, 'sucess'

def get_checked_name():
    global ads_dict
    if len(ads_dict)== 0:
        read_data()
    return ads_dict.keys()

def save_ads_modify():
    global ads_dict
    with open(ads_data_file, 'w') as f:
        f.write(json.dumps(ads_dict))

