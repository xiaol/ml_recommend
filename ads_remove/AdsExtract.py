# -*- coding: utf-8 -*-
# @Time    : 16/10/9 下午2:41
# @Author  : liulei
# @Brief   : 
# @File    : AdsExtract.py.py
# @Software: PyCharm Community Edition
import json
import os
import jieba
import json
import os
import psycopg2
import re
import logging
#定义日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('ads_dect_log.txt')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


real_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
POSTGRE_USER = 'postgres'
POSTGRE_PWD = 'ly@postgres&2015'
POSTGRE_HOST = '120.27.163.25'
POSTGRE_DBNAME = 'BDP'
POSTGRES = "postgresql://postgres:ly@postgres&2015@120.27.163.25/BDP"
def get_postgredb():
    connection = psycopg2.connect(database=POSTGRE_DBNAME, user=POSTGRE_USER, password=POSTGRE_PWD, host=POSTGRE_HOST,)
    cursor = connection.cursor()
    return connection, cursor

##替换常用HTML字符实体.
# 使用正常的字符替换HTML中特殊的字符实体.
# 你可以添加新的实体字符到CHAR_ENTITIES中,处理更多HTML字符实体.
# @param htmlstr HTML字符串.
def replaceCharEntity(htmlstr):
    CHAR_ENTITIES = {'nbsp': ' ', '160': ' ',
                     'lt': '<', '60': '<',
                     'gt': '>', '62': '>',
                     'amp': '&', '38': '&',
                     'quot': '"', '34': '"',}

    re_charEntity = re.compile(r'&#?(?P<name>\w+);')
    sz = re_charEntity.search(htmlstr)
    while sz:
        entity = sz.group()  # entity全称，如&gt;
        key = sz.group('name')  # 去除&;后entity,如&gt;为gt
        try:
            htmlstr = re_charEntity.sub(CHAR_ENTITIES[key], htmlstr, 1)
            sz = re_charEntity.search(htmlstr)
        except KeyError:
            # 以空串代替
            htmlstr = re_charEntity.sub('', htmlstr, 1)
            sz = re_charEntity.search(htmlstr)
    return htmlstr

##过滤HTML中的标签
# 将HTML中标签等信息去掉
# @param htmlstr HTML字符串.
def filter_tags(htmlstr):
    # 先过滤CDATA
    re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.I)  # 匹配CDATA
    re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
    re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
    re_br = re.compile('<br\s*?/?>')  # 处理换行
    re_h = re.compile('</?\w+[^>]*>')  # HTML标签
    re_comment = re.compile('<!--[^>]*-->')  # HTML注释
    s = re_cdata.sub('', htmlstr)  # 去掉CDATA
    s = re_script.sub('', s)  # 去掉SCRIPT
    s = re_style.sub('', s)  # 去掉style
    s = re_br.sub('\n', s)  # 将br转换为换行
    s = re_h.sub('', s)  # 去掉HTML 标签
    s = re_comment.sub('', s)  # 去掉HTML注释
    # 去掉多余的空行
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    s = replaceCharEntity(s)  # 替换实体
    return s

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
    return wechat_set

#wechat_set = get_wechatnum_name()

def get_para_list(paras_dict_list, nid=None, only_txt=True):
    para_list = []
    if nid:
        para_list.append(str(nid))
    for para_dict in paras_dict_list:
        for item in para_dict.items():
            if only_txt:
                if item[0] == 'txt':
                    para_list.append(filter_tags(item[1]))
            else:
                para_list.append(item[1])
    return para_list

t = ["网络大电影", "华人瞰世界", "美国咖", "汽车爱好者", "母婴头条", "新闻夜航", "男人装", "政商参阅", "占豪", "北京知道", "严肃八卦", "利维坦",
     "李跃儿芭学园", "八卦芒果", "Instagram", "我们爱历史", "纽约华人在线", "利维坦", "纽约华人在线"]#, "利维坦", "不二大叔", "新世相",
     #"CCTV今日说法", "深焦DeepFocus", "粥悦悦", "素食星球","最潮穿衣搭配","天天家常菜谱", "神吐槽vs神回复", "搞趣", "都市快报", "X博士"]
#t = ["网络大电影", "华人瞰世界", "美国咖", "汽车爱好者", "利维坦", "不二大叔", "新世相", "桃红梨白"]
t = ["桃红梨白"]
test_name = set(t)
def get_wechat_news(wechat_name_set = get_wechatnum_name()):
#def get_wechat_news(wechat_name_set = test_name):
    conn, cursor = get_postgredb()
    news_dict = dict()
    sql_get_wechat_news = "select nid, title, content from newslist_v2 where pname = \'{0}\' LIMIT 30"
    for name in wechat_name_set:  #etc:name = "美国咖"
        print name
        cursor.execute(sql_get_wechat_news.format(name))
        rows = cursor.fetchall()
        news_dict[name] = list()
        for news in rows:
            nid = news[0]
            news_dict[name].append(get_para_list(news[2], nid))
    conn.close()
    return news_dict

sql_get_news = "select content,pname from newslist_v2 where nid = {0}"
def get_para_on_nid(nid):
    global ads_dict
    conn, cursor = get_postgredb()
    cursor.execute(sql_get_news.format(nid))
    rows = cursor.fetchall()
    if len(rows) == 0:
        return '', []
    row = rows[0]
    contents = row[0]
    pname = row[1]
    content_list = get_para_list(contents)
    conn.close()
    return pname, content_list

#删除线上新闻的广告
#@input: ads_dict---检测出来的广告段落。 开头广告的终止段落和结尾广告的开始段落
def remove_ads(ads_dict, nid):
    conn, cursor = get_postgredb()
    cursor.execute(sql_get_news.format(nid))
    rows = cursor.fetchall()
    row = rows[0]
    contents = row[0]

    for item in ads_dict.items():
        ads_txt = item[1]
        #开头广告段落号
        if int(item[0].encode('utf-8')) >= 0:
            n = 0
            while n < len(contents):
                elem = contents[n]
                if 'txt'.encode('utf-8') not in elem.keys():
                    contents.remove(elem)
                    continue
                valid_elem = filter_tags(elem['txt'])
                contents.remove(elem)
                if valid_elem == ads_txt:
                    break
        else:  #结尾广告段落号
            for i in xrange(len(contents)-1, -1, -1):
                if 'txt'.encode('utf-8') not in contents[i].keys():
                    contents.remove(contents[i])
                    continue
                valid_elem = filter_tags(contents[i]['txt'])
                contents.remove(contents[i])
                if valid_elem == ads_txt:
                    break

    if len(contents) == 0: #全文都是广告
        update_sql = "update newslist_v2 set state = 1 where nid = {1}"
        sql = update_sql.format(json.dumps(contents).encode('utf-8'), nid)
        cursor.execute(sql)
        conn.commit()
        conn.close()
        return
    print 'remove ads of ' + nid
    logger.info("remove ads of " + nid)
    update_sql = "update newslist_v2 set content=\'{0}\' where nid = {1}"
    sql = update_sql.format(json.dumps(contents).encode('utf-8'), nid)
    cursor.execute(sql)
    conn.commit()
    conn.close()

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
from multiprocessing import Lock, Manager, Pool
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
                el = item[0] + '\t|' + ''.join(item[1][0].split()) #item[1][0]是段落字符串, item[1][1]是nid
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
        #if float(i[1]) > float((num * (num -1))/100):
        if float(i[1]) > float(num/5):
            para = i[0].split('\t|')
            tmp_list.append( (int(para[0]), para[1], ads_nid_dict[i[0]]) )
    sorted_list = sorted(tmp_list, key=lambda d:d[0])
    ads_list = []
    for para in sorted_list:
        ads_list.append( (int(para[0]), para[1], para[2], 1)) #第四个元素1表示checked。当页面上去选时,置为0

    coll_result(name, ads_list)

real_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
ads_data_file = real_path + '/../result/ads_data.txt'
ads_update_data_file = real_path + '/../result/ads_update_data.txt'
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
#ads_dict = Manager().dict()
#读取广告数据. ads_dict.keys()类型是unicode
def read_data():
    global ads_dict
    with open(ads_data_file, 'r') as f:
        r = f.read()
        if len(r) > 0:
            ads_dict = json.loads(r)

#读取最新的广告数据
ads_update_dict = {}
def read_update_data():
    global ads_update_dict
    with open(ads_update_data_file, 'r') as f:
        r = f.read()
        if len(r) > 0:
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
    #替换原先的文件
    ads_dict = ads_update_dict
    save_ads_modify()

def get_ads_of_one_wechat(name):
    global ads_dict
    if len(ads_dict) == 0:
        read_data()
    return ads_dict[name]

#为基于nid获取广告提供的方法, ads_dict.keys()是unicode 类型; content_list 和ads_dict[pname]字符串都是unicode
def get_ads_paras(pname, content_list):
    global ads_dict
    if len(ads_dict) == 0:
        read_data()
    if pname not in ads_dict:
        return {}
    ads_paras = ads_dict[pname]

    i = 0
    to_remove = []
    while i < len(ads_paras):
        if ads_paras[i][0] < 0: #结尾有广告
            #如果被手动干预判定为非广告,则跳过
            if int(ads_paras[i][3]) == 0:
                i += 1
                continue
            para = ads_paras[i]
            n = int(para[0])
            if len(content_list) < abs(n):
                i += 1
                continue
            #content = content_list[n]
            content = ''.join(content_list[n].split())
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
                #如果被手动干预判定为非广告,则跳过
                if int(ads_paras[k][3]) == 0:
                    k -= 1
                    continue
                para = ads_paras[k]
                n = int(para[0])
                if len(content_list) < abs(n):
                    k -= 1
                    continue
                content = ''.join(content_list[n].split())
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
    return result

#modify_type: 操作类型,指添加还是删除
#modify_data: 操作数据. 结构:   微信号名称:段落号:段落内容
def modify_ads_results(modify_type, modify_data):
    global ads_dict
    data = modify_data.split(':')
    #name = data[0].decode('utf-8')
    name = data[0]
    if len(data) < 3 or name not in ads_dict.keys():
        return False, 'error input data'
    p_num = data[1]
    p_text = data[2]
    paras = ads_dict[name]
    if modify_type == 'delete':
        for item in paras:
            para = ''.join(item[1].split())
            #para2 = ''.join(p_text.decode('utf-8').split())
            para2 = ''.join(p_text.split())
            if item[0] == int(p_num) and para == para2:
                print 'delete'
                item[3] = 0
                break
    elif modify_type == 'add':
        for item in paras:
            para = ''.join(item[1].split())
            if item[0] == int(p_num) and para == ''.join(p_text.split()):
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

