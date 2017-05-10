# -*- coding: utf-8 -*-
# @Time    : 16/10/8 上午10:17
# @Author  : liulei
# @Brief   : 
# @File    : AdsParagraphExtract2.py.py
# @Software: PyCharm Community Edition
import os
import jieba
from DocPreProcess import get_postgredb
from DocPreProcess import filter_tags

real_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
ads_data_file = real_path + '/../result/ads_data.txt'

#微信公众号从自媒体中取得
sql_get_wechat = "select sname from sourcelist_v2 where queue='spider:news:wechat:start_urls'"
def get_wechatnum_name():
    wechat_set = set()
    conn, cursor = get_postgredb()
    cursor.execute(sql_get_wechat)
    rows = cursor.fetchall()
    for elem in rows:
        sname_and_id = elem[0]
        name = sname_and_id.split(';')[0]
        wechat_set.add(name)
    conn.close()
    print 'get wechat finished'
    return wechat_set

def get_para_list(paras_dict_list):
    para_list = []
    for para_dict in paras_dict_list:
        for item in para_dict.items():
            if item[0] == 'txt':
                para_list.append(item[1])
    return para_list

#定义两个句子内容相同
def is_sentenses_same(s1, s2):
    s1_words = jieba.cut(s1)
    s2_words = jieba.cut(s2)
    n = 0
    l1 = 0
    l2 =0
    List1 = []
    List2 = []
    for w in s1_words:
        l1 += 1
        List1.append(w)
    for w in s2_words:
        l2 += 1
        List2.append(w)
    for w in List1:
        if w in List2:
            n += 1
    if n > 0 and float(n) > float(l1 + l2)/2 * 0.8:
        return True
    else:
        return False

#求两篇新闻的相同段落
#{
#    0:"关注搞趣微信号，每天推送最搞笑内涵的"   #形同的行号及段落内容
#    -1:"喜欢就点赞吧"
# }
def get_same_paras(paras1, paras2):
    same_para_info = {}
    N1 = len(paras1)
    N2 = len(paras2)
    n1 = 0
    same_num = 0
    for p1_with_html in paras1:
        n2 = 0
        p1 = filter_tags(p1_with_html)
        bP1Finished = False
        for p2_with_html in paras2:
            p2 = filter_tags(p2_with_html)
            #if p1 == p2:
            if is_sentenses_same(p1, p2):
                #if (n2 == 0 and n1 == 0) or (n2 <5 and n1 < 5 and n2==n1): #位于两篇文章开头
                if n2 < 5 and n1 < 5:
                    if n2 == n1:
                        same_para_info[str(n1)] = p1
                    else:
                        same_para_info[str(n1)] = p1
                        same_para_info[str(n2)] = p2
                elif n1 - N1 == n2 - N2: #位于文章结尾
                    same_para_info[str(n1 - N1)] = p1
                else:
                    same_para_info[str(n1 - N1)] = p1
                    same_para_info[str(n2 - N2)] = p2
                same_num += 1
                bP1Finished = True
            else:
                pass
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

from multiprocessing import Process, Lock, Manager, Pool
mylock = Lock()
def write_to_file(s):
    mylock.acquire()
    f = open(ads_data_file, 'w')
    f.write(s)
    f.close()
    mylock.release()

#处理微信公众号的进程
def get_ads_proc(wechat_name):
    print 'process of ', wechat_name
    conn, cursor = get_postgredb()
    sql_get_wechat_news = "select nid, title, content from newslist_v2 where pname = \'{0}\' LIMIT 30"
    cursor.execute(sql_get_wechat_news.format(wechat_name))
    rows = cursor.fetchall()
    para_list_list = []
    nids = []
    same_content_dict = dict()
    for news in rows:
        nid = news[0]
        nids.append(nid)
        paras = news[2]
        para_list = get_para_list(paras)
        para_list_list.append(para_list)
    print nids
    ads_dict = {} #一个公众号的所有相同段落
    i = 0
    num = len(para_list_list)
    while i < len(para_list_list):
        paras = para_list_list[i]
        k = i + 1
        while k < len(para_list_list):
            paras2 = para_list_list[k]
            bSameNews, same_dict = get_same_paras(paras, paras2)
            if bSameNews:
                del para_list_list[k]
                #f2.write(str(nids[i]) + ' --- same with ---' + str(nids[k]))
                #same_content_dict[str(nids[i])] = str(nids[k])
            for elem in same_dict.items():
                el = elem[0] + '\t|' + elem[1]
                if el not in ads_dict:
                    ads_dict[el] = 1
                else:
                    ads_dict[el] += 1
            if len(same_dict):
                del para_list_list[k]
            else:
                k += 1
        i += 1
    sorted_dict = sorted(ads_dict.items(), key=lambda d:d[1], reverse=True)

    s = wechat_name + ' '
    #f.write(name + ' ')
    #wechat_para_dict[name] = []
    tmp_list = []
    for i in sorted_dict:
        if float(i[1]) > float(num/3):
            para = i[0].split('\t|')
            tmp_list.append( (int(para[0]), para[1]) )
    sorted_list = sorted(tmp_list, key=lambda d:d[0])
    for para in sorted_list:
        para_num = int(para[0])
        para_text = para[1]
        #f.write(str(para_num)+ ':' + ''.join(para_text.split()) + ' ')
        s += str(para_num)+ ':' + ''.join(para_text.split()) + ' ';
        #wechat_para_dict[name].append((para_num, para_text));
    #f.write('\n')
    s += '\n'
    write_to_file(s)
    conn.close()





#公众号各取100篇新闻
#dict{"美国咖":dict{nid1:[段落1, 段落2...],
#                  nid2:[段落1, 段落2...]...},
#     "网络大电影":dict{nid3:[段落1, 段落2...],
#                  nid4:[段落1, 段落2...]}
def get_wechat_news(name_set):
    news_dict = dict()
    conn, cursor = get_postgredb()
    #wechat_para_dict = {}
    pool = Pool(10)
    for name in name_set:  #etc:name = "美国咖"
        pool.apply_async(get_ads_proc, name)
    #return wechat_para_dict


def read_ads_file():
    file = open(ads_data_file, 'r')
    ads_dict = {}
    for line in file.readlines():
        line_sp = line.split()
        if len(line_sp) < 2:   #没有广告段落
            continue
        wechat_name = line_sp[0]
        ads_dict[wechat_name] = []
        for i in xrange(1, len(line_sp)):
            pair = line_sp[i].split(':')
            n = int(pair[0])
            txt = pair[1]
            ads_dict[wechat_name].append((n, txt))
    return ads_dict

t = ["网络大电影", "华人瞰世界", "美国咖", "汽车爱好者", "母婴头条", "新闻夜航", "男人装", "政商参阅", "占豪", "北京知道", "严肃八卦", "利维坦",
     "李跃儿芭学园", "八卦芒果", "Instagram", "我们爱历史", "纽约华人在线"]
test_name = set(t)
wechat_name_set = get_wechatnum_name()
#ads_dict = get_wechat_news(test_name)
get_wechat_news(test_name)
#ads_dict = read_ads_file()

def get_ads_on_nid(nid):
    global ads_dict
    sql_get_news = "select content,pname from newslist_v2 where nid = {0}"
    conn, cursor = get_postgredb()
    cursor.execute(sql_get_news.format(nid))
    rows = cursor.fetchall()
    row = rows[0]
    contents = row[0]
    pname = row[1]
    content_list = get_para_list(contents)
    ads_paras = ads_dict[pname]
    sorted_ads = sorted(ads_paras, key=lambda d:d[0])
    print sorted_ads
    remove_para_list = []
    i = 0
    while i < len(sorted_ads):
        para = sorted_ads[i]
        if para[0] < 0:
            n = int(para[0])
            if len(content_list) < abs(n):
                continue
            str_no_tags = filter_tags(content_list[int(para[0])])
            if is_sentenses_same(str_no_tags, para[1]):
                remove_para_list.append(para[0])
                for k in xrange(i+1, len(sorted_ads)):
                    if sorted_ads[k][0] < 0:
                        i = k + 1
                        continue
                    else:
                        i = k
                        break
            else:
                i += 1
                continue
        else:
            k = len(sorted_ads) - 1
            while k >= i:
                para2 = sorted_ads[k]
                n = int(para2[0])
                if len(content_list) < abs(n):
                    continue
                str_no_tags = filter_tags(content_list[int(para2[0])])
                if is_sentenses_same(str_no_tags, para2[1]):
                    remove_para_list.append(para2[0])
                    break
                else:
                    k -= 1
            break
    print remove_para_list
    result = {}
    for i in remove_para_list:
        if int(i) < 0:
            result.update({'End Ads:':content_list[int(i)]})
        else:
            result.update({'Begin Ads:':content_list[int(i)]})
    res = {}
    res[nid] = result
    return res


def test():
    #sql_test = "select content from newslist_v2 where nid in (7057708, 7058238)"
    sql_test = "select content from newslist_v2 where nid in (7088831, 7088826)"
    conn, cursor = get_postgredb()
    cursor.execute(sql_test)
    rows = cursor.fetchall()
    r1 = rows[0]
    r2 = rows[1]
    c1 = r1[0]
    c2 = r2[0]
    p1 = get_para_list(c1)
    p2 = get_para_list(c2)
    p1_no_html = []
    p2_no_html = []
    for s in p1:
        p1_no_html.append(filter_tags(s))
    for s in p2:
        p2_no_html.append(filter_tags(s))
    r = get_same_paras(p1, p2)


#test()

