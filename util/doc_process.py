# -*- coding: utf-8 -*-
# @Time    : 16/12/1 上午11:15
# @Author  : liulei
# @Brief   : 
# @File    : doc_process.py
# @Software: PyCharm Community Edition
import os
import re
import jieba
import jieba.analyse
import time
import math
import pandas as pd
##过滤HTML中的标签
# 将HTML中标签等信息去掉
# @param htmlstr HTML字符串.
import psycopg2
import traceback
from bs4 import BeautifulSoup

sentence_delimiters = ['?', '!', ';', '？', '！', '。', '；', '……', '…', '\n']
question_delimiters = [u'?', u'？']
news_text_nid_sql = "select nid, title, content from newslist_v2 where nid={0}"


#使用BeautifulSoup提取内容
#@output:
def get_paragraph_text(para):
    soup = BeautifulSoup(para, 'lxml')
    return soup.text


def is_num(str):
    try:
        float(str)
        return True
    except:
        return False


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

def filter_url(str):
    if str.find('www') < 0 and str.find('http') < 0:
        return str
    myString_list = [item for item in str.split(" ")]
    url_list = []
    for item in myString_list:
        try:
            url_list.append(re.search("(?P<url>https?://[^\s]+)", item).group("url"))
        except:
            pass
    for i in url_list:
        str = str.replace(i, ' ')
    return str

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


def get_file_real_path():
    return os.path.realpath(__file__)


def get_file_real_dir_path():
    return os.path.split(os.path.realpath(__file__))[0]

real_dir_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
net_words_file = real_dir_path + '/networds.txt'
stop_words_file = real_dir_path + '/stopwords.txt'
stopwords = {}.fromkeys([line.rstrip() for line in open(stop_words_file)]) #utf-8
stopwords_set = set(stopwords)
#去除html标签及停用词
def remove_html_and_stopwords(str):
    #删除html标签
    txt_no_html = filter_tags(str)
    jieba.load_userdict(net_words_file)
    words = jieba.cut(txt_no_html)  #unicode is returned
    final_words = []
    for w in words:
        if not w.encode('utf-8') in stopwords_set and (not w.isspace()):
            final_words.append(w)
    return final_words

def filterPOS2(org_text, pos_list):
    txtlist = []
    # 去除特定词性的词
    for w in org_text:
        if w.flag in pos_list:
            pass
        else:
            txtlist.append(w.word)
    return txtlist

POS = ['zg', 'uj', 'ul', 'e', 'd', 'uz', 'y']
#去除html标签及停用词并筛选词性
def filter_html_stopwords_pos(str, remove_num=False, remove_single_word=False, return_no_html=False, remove_html=True):
    #删除html标签
    str = ''.join(str.split())
    if remove_html:
        txt_no_html = filter_tags(str)
    else:
        txt_no_html = str
    import jieba.posseg
    jieba.load_userdict(net_words_file)
    words = jieba.posseg.cut(txt_no_html)  #unicode is returned
    words_filter = filterPOS2(words, POS)
    final_words = []
    for w in words_filter:
        if not w.encode('utf-8') in stopwords_set and (not w.isspace()):
            final_words.append(w)
    i = 0
    if remove_num == True:
        while i < len(final_words):
            w = final_words[i]
            if is_num(w):
                final_words.remove(w)
                continue
            else:
                i += 1
    i = 0
    if remove_single_word == True:
        while i < len(final_words):
            w = final_words[i]
            if len(w) == 1:
                final_words.remove(w)
                continue
            else:
                i += 1

    if return_no_html:
        return txt_no_html, final_words
    return final_words


#jieba提取关键词
def jieba_extract_keywords(str, K):
    #删除html标签
    txt_no_html = filter_tags(str)
    jieba.load_userdict(net_words_file)
    jieba.analyse.set_stop_words(stop_words_file)
    words = jieba.analyse.extract_tags(txt_no_html, K)
    return words

POSTGRE_USER = 'postgres'
POSTGRE_PWD = 'ly@postgres&2015'
#POSTGRE_HOST = '120.27.163.25'
POSTGRE_HOST = '10.47.54.175'
POSTGRE_DBNAME = 'BDP'
#POSTGRES = "postgresql://postgres:ly@postgres&2015@120.27.163.25:5432/BDP"
POSTGRES = "postgresql://postgres:ly@postgres&2015@10.47.54.175:5432/BDP"
def get_postgredb():
    try:
        connection = psycopg2.connect(database=POSTGRE_DBNAME, user=POSTGRE_USER, password=POSTGRE_PWD, host=POSTGRE_HOST,)
        cursor = connection.cursor()
        return connection, cursor
    except:    #出现异常,再次连接
        try:
            time.sleep(2)
            connection = psycopg2.connect(database=POSTGRE_DBNAME, user=POSTGRE_USER, password=POSTGRE_PWD, host=POSTGRE_HOST,)
            cursor = connection.cursor()
            return connection, cursor
        except:
            traceback.print_exc()
            raise


#数据库查询从节点
#POSTGRE_HOST_QUERY = '120.27.162.201'
POSTGRE_HOST_QUERY = '10.47.54.32'
POSTGRE_DBNAME_QUERY = 'BDP'
#POSTGRES_QUERY = "postgresql://postgres:ly@postgres&2015@120.27.162.201:5432/BDP"
POSTGRES_QUERY = "postgresql://postgres:ly@postgres&2015@10.47.54.32:5432/BDP"
def get_postgredb_query():
    try:
        connection = psycopg2.connect(database=POSTGRE_DBNAME_QUERY, user=POSTGRE_USER, password=POSTGRE_PWD, host=POSTGRE_HOST_QUERY,)
        cursor = connection.cursor()
        return connection, cursor
    except:    #出现异常,再次连接
        try:
            time.sleep(2)
            connection = psycopg2.connect(database=POSTGRE_DBNAME_QUERY, user=POSTGRE_USER, password=POSTGRE_PWD, host=POSTGRE_HOST_QUERY,)
            cursor = connection.cursor()
            return connection, cursor
        except:
            traceback.print_exc()
            raise



'''
nid_sql = 'select a.title, a.content, c.cname \
from (select * from newslist_v2 where nid=%s) a \
inner join channellist_v2 c on a."chid"=c."id"'
'''
nid_sql = "select title, content from info_news where nid=%s "
#获取nid的段落的字符串。
def get_words_on_nid(nid):
    conn, cursor = get_postgredb_query()
    cursor.execute(nid_sql, [nid])
    rows = cursor.fetchall()
    word_list = []
    for row in rows:
        title = row[0]  #str类型
        content_list = row[1]
        txt = ''
        for content in content_list:
            if 'txt' in content.keys():
                txt += content['txt']
        total_txt = title + txt.encode('utf-8')
        word_list = filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
    return word_list


# 检查某字符是否分句标志符号的函数；如果是，返回True，否则返回False
def FindToken(cutlist, char):
    if char in cutlist:
        return True
    else:
        return False

        # 进行分句的核心函数


# 设置分句的标志符号；可以根据实际需要进行修改
#cutlist = "。！？".decode('utf-8')
#cutlist = "。！？.!?。!?".decode('utf-8')
cutlist = "。！？!?。!?".decode('utf-8') #不包含英文句号,因为也会被当成小数点
def Cut(lines, cutlist=cutlist):  # 参数1：引用分句标志符；参数2：被分句的文本，为一行中文字符
    l = []  # 句子列表，用于存储单个分句成功后的整句内容，为函数的返回值
    line = []  # 临时列表，用于存储捕获到分句标志符之前的每个字符，一旦发现分句符号后，就会将其内容全部赋给l，然后就会被清空


    if lines.find('http') > 0:
        myString_list = [item for item in lines.split(" ")]
        for item in myString_list:
            try:
                url = re.search("(?P<url>https?://[^\s]+)", item).group("url")
                l.append(url)
                lines = lines.replace(url, ' ')
            except:
                pass

    sentence_len = 0
    for i in lines:  # 对函数参数2中的每一字符逐个进行检查 （本函数中，如果将if和else对换一下位置，会更好懂）
        if FindToken(cutlist, i):
            if sentence_len == 0:  # 如果当前字符是分句符号,并且不是一个句子的开头
                continue
            line.append(i)  # 将此字符放入临时列表中
            l.append(''.join(line))  # 并把当前临时列表的内容加入到句子列表中
            line = []  # 将符号列表清空，以便下次分句使用
            sentence_len = 0
        else:  # 如果当前字符不是分句符号，则将该字符直接放入临时列表中
            line.append(i)
            sentence_len += 1
    if len(line) != 0:
        l.append(''.join(line))
    return l


#获取文章段落
def get_sentences_on_nid(nid):
    conn, cursor = get_postgredb_query()
    cursor.execute(news_text_nid_sql.format(nid))
    rows = cursor.fetchall()
    sentences = []
    for r in rows:
        title = r[1]
        content_list = r[2]
        for elems in content_list: #段落
            if "txt" in elems.keys():
                l = Cut(filter_tags(elems['txt']))
                sentences.extend(l)

    return sentences

def join_file(in_filenames, out_filename):
    out = open(out_filename, 'w+')

    for f in in_filenames:
        try:
            in_file = open(f, 'r')
            out.write(in_file.read())
            in_file.close()
        except IOError:
            raise
    out.close()


def join_csv(in_files, out_file, columns):
    import pandas as pd
    df = pd.DataFrame(columns=columns)
    for f in in_files:
        print '^^^ ' + f
        d = pd.read_csv(f)
        df = df.merge(d, how='outer')
    df.to_csv(out_file, index=False)


#allow_pos = ('a', 'g', 'n', 'nr', 'ns', 'nt', 'ng', 'nl','nz',
#             't', 's', 'v', 'vd', 'vg', 'vl', 'vi', 'vx', 'vf', 'vn', 'z', 'i', 'j', 'l', 'eng')

allow_pos = ('a', 'n', 'v', 'eng', 's', 't', 'i', 'j', 'l', 'z')

def cut_pos_jieba(doc, topK = 20):
    s = ''.join(doc.split())
    s = filter_tags(s)
    jieba.load_userdict(net_words_file)
    jieba.analyse.set_stop_words(stop_words_file)
    tags = jieba.analyse.extract_tags(s, topK=topK, withWeight=False, allowPOS=allow_pos)
    return ' '.join(tags).encode('utf-8')


import pynlpir
def open_pynlpir():
    global pynlpir
    pynlpir.open()


def close_pynlpir():
    global pynlpir
    pynlpir.close()


allow_pos_nlpir = ('a', 'i', 'j', 'n', 'nd', 'nh', 'ni', 'nl', 'ns', 'nt', 'nz', 'v', 'ws')
from bs4 import BeautifulSoup
def cut_pos_nlpir(doc, topK = 20):
    #s = filter_tags(doc)
    soup = BeautifulSoup(doc, 'lxml')
    s = soup.get_text()
    try:
        s = ''.join(s.split())
        ws = pynlpir.get_key_words(s, topK)
        return ' '.join(ws).encode('utf-8')
    except:
        print 'error:  ' + s
        traceback.print_exc()
        raise



from pyltp import Segmentor, Postagger
segmentor = Segmentor()
segmentor.load('/root/git/ltp_data/cws.model')
#segmentor.load('/Users/a000/git/ltp_data/cws.model')
poser = Postagger()
poser.load('/root/git/ltp_data/pos.model')
#poser.load('/Users/a000/git/ltp_data/pos.model')

allow_pos_ltp = ('a', 'i', 'j', 'n', 'nh', 'ni', 'nl', 'ns', 'nt', 'nz', 'v', 'ws')
#使用哈工大pyltp分词, 过滤词性
def cut_pos_ltp(doc, filter_pos = True, allow_pos = allow_pos_ltp, remove_tags=True):
    s = ''.join(doc.split())  #去除空白符
    if remove_tags:
        s = filter_tags(s)  #去除html标签
    words = segmentor.segment(s)

    words2 = []
    for w in words:
        if len(w.decode('utf-8')) > 1 and (w not in stopwords_set):
            words2.append(w)
    if not filter_pos:
        return ' '.join(words2)

    poses = poser.postag(words2)
    ss = []
    for i, pos in enumerate(poses):
        if pos in allow_pos:
            ss.append(words2[i])
    return ' '.join(ss)


#获取idf
def get_idf(docs, save_path):
    N = len(docs)
    word_num_dict = {}
    for doc in docs:
        ws = doc.split()
        ws_set = set(ws)
        for w in ws_set:
            if w not in word_num_dict:
                word_num_dict[w] = 1
            else:
                word_num_dict[w] += 1
    word_idf_dict = {}
    for item in word_num_dict.items():
        word_idf_dict[item[0]] = math.log(float(N) / item[1])
    f = open(save_path, 'w')
    for item in word_idf_dict.items():
        f.write(item[0] + ' ' +  str(item[1]) + '\n')


################################################################################
#提取关键词
#@param :
#      idf_path : 用户指定Idf文件
#      docs: 句子的list.
#      topK :做大关键词数目
#       max_percent:保留的最大比例。   实际词的个数为min(topK, max_percent*len)
#      single:是否保留词频。
#@output: 返回词列表
################################################################################
def extract_keywords(idf_path, docs, topK=20, max_percent=1., single=False):
    if not os.path.isfile(idf_path):
        raise Exception("extract_keywords: idf file does not exit: " + idf_path)
    f = open(idf_path, 'r')
    lines = f.readlines()
    word_idf = {}
    for line in lines:
        w_idf = line.split(' ')
        word_idf[w_idf[0]] = float(w_idf[1])
    all_keywords = []
    n = 0
    for doc in docs:  #每一篇文本
        try:
            n += 1
            words = doc.split()
            w_tfidf = dict()
            for w in words: #每一个词
                if len(w.decode('utf-8')) < 2 or (w not in word_idf) or (w in w_tfidf.keys()):
                    continue
                w_tfidf[w] = (float(words.count(w)) / len(w)) * word_idf[w]
            tags = sorted(w_tfidf.items(), key=lambda d: d[1], reverse=True)
            k = min(topK, int(len(words) * max_percent))
            ws_k = tags[:k]
            ws_k = [t[0] for t in ws_k]
            if single:
                all_keywords.append(' '.join(ws_k))
            else:
                kw = []
                for w in words:
                    if w in ws_k:
                        kw.append(w)
                all_keywords.append(' '.join(kw))
        except:
            traceback.print_exc()
            continue
    return all_keywords


################################################################################
#@brief : 各个频道获取新闻, 保存至csv文件
#@input : chnl_num_dict------各频道取新闻数量
#          save_dir--------csv保存目录
################################################################################
'''
channle_sql ='SELECT a.title, a.content, a.nid \
FROM newslist_v2 a \
INNER JOIN (select * from channellist_v2 where "cname"=%s) c \
ON \
a."chid"=c."id" where a.state=0 ORDER BY nid desc LIMIT %s'
'''
channle_sql = "select ni.title, ni.content, ni.nid from info_news ni " \
              "inner join channellist_v2 c on ni.chid=c.id " \
              "inner join newslist_v2 nv on ni.nid=nv.nid " \
              "where c.cname=%s and nv.state=0 " \
              "order by ni.nid desc limit %s"
def coll_news(chnl_num_dict, save_dir, to_csv=True):
    import pandas as pd
    conn, cursor = get_postgredb_query()
    chnls = []
    nids = []
    docs = []
    for item in chnl_num_dict.items(): #每个频道
        print '*****' + item[0]
        cursor.execute(channle_sql, (item[0], item[1]))
        rows = cursor.fetchall()
        for row in rows:
            title = row[0]
            content_list = row[1]
            txt = ''
            for content in content_list:
                if 'txt' in content.keys():
                    txt += content['txt'] + ' '   #unicode

            soup = BeautifulSoup(txt, 'lxml')
            txt = soup.get_text()
            total_txt = title + ' ' + txt.encode('utf-8')
            chnls.append(item[0])
            nids.append(row[2])
            docs.append(''.join(total_txt.split())) #split主要去除回车符\r, 否则pandas.read_csv出错
    if to_csv:
        data = {'chnl':chnls, 'nid':nids, 'doc':docs}
        df = pd.DataFrame(data, columns=('chnl', 'nid', 'doc'))
        df.to_csv(os.path.join(save_dir, 'raw.csv'), index=False)
    conn.close()
    return chnls, nids, docs


def coll_news_cut(chnl_num_dict, save_dir, save_raw_to_csv=True, to_csv_file=True):
    import pandas as pd
    chnls, nids, docs = coll_news(chnl_num_dict, save_dir, save_raw_to_csv)
    docs = pd.Series(docs)
    docs = docs.apply(cut_pos_ltp, (True, allow_pos_ltp, False))
    df = pd.DataFrame({'chnl':chnls, 'nid':nids, 'doc':docs}, columns=('chnl', 'nid', 'doc'))
    if to_csv_file:
        df.to_csv(os.path.join(save_dir, 'cut.csv'), index=False)
    return df


def coll_cut_extract(chnl_num_dict,
                     save_dir,
                     idf_save_path,
                     topK=50,
                     max_percent=0.3,
                     save_raw_to_csv=True,
                     save_cut_to_csv=True,
                     to_csv_file=True):
    import pandas as pd
    df = coll_news_cut(chnl_num_dict, save_dir, save_raw_to_csv, save_cut_to_csv)
    doc = df['doc']
    from sklearn.feature_extraction.text import TfidfVectorizer
    tfidf_vec = TfidfVectorizer(use_idf=True, smooth_idf=False, max_features=100000)
    tfidf = tfidf_vec.fit_transform(doc)
    idf_dict = dict(zip(tfidf_vec.get_feature_names(), tfidf_vec.idf_))
    features = []
    idfs = []
    for item in idf_dict.items():
        features.append(item[0].encode('utf-8'))
        idfs.append(item[1])
    idf_df = pd.DataFrame({'feature': features, 'idf': idfs}, index=None)
    idf_df.to_csv(idf_save_path, index=False, header=False, sep=' ')
    all_keywords = extract_keywords(idf_save_path, doc.tolist(), topK, max_percent)
    df = pd.DataFrame({'chnl': df['chnl'], 'nid': df['nid'], 'doc': all_keywords}, columns=('chnl', 'nid', 'doc'))
    if to_csv_file:
        df.to_csv(os.path.join(save_dir, 'cut_extract.csv'), index=False)


#########################  提供多进程版本  ####################################
#读取频道新闻
def coll_chnal(chname, num, to_csv=True, save_path=''):
    import pandas as pd
    conn, cursor = get_postgredb_query()
    chnal = []
    nids = []
    docs = []
    cursor.execute(channle_sql, (chname, num))
    rows = cursor.fetchall()
    for row in rows:
        title = row[0]
        content_list = row[1]
        txt = ''
        for content in content_list:
            if 'txt' in content.keys():
                txt += content['txt'] + ' '   #unicode

        soup = BeautifulSoup(txt, 'lxml')
        txt = soup.get_text()
        total_txt = title + ' ' + txt.encode('utf-8')
        #去除三种特殊空格
        total_txt = total_txt.replace('\xe2\x80\x8b', '')
        total_txt = total_txt.replace('\xe2\x80\x8c', '')
        total_txt = total_txt.replace('\xe2\x80\x8d', '')

        chnal.append(chname)
        nids.append(row[2])
        docs.append(''.join(total_txt.split())) #split主要去除回车符\r, 否则pandas.read_csv出错
    if to_csv:
        data = {'chnl':chnal, 'nid':nids, 'doc':docs}
        df = pd.DataFrame(data, columns=('chnl', 'nid', 'doc'))
        df.to_csv(save_path, index=False)
    conn.close()


def get_idf_file(docs, idf_save_path, max_features=99999999999):
    from sklearn.feature_extraction.text import TfidfVectorizer
    #tfidf_vec = TfidfVectorizer(use_idf=True, smooth_idf=False, max_df=0.001, min_df=0.00001, max_features=100000)
    tfidf_vec = TfidfVectorizer(use_idf=True, smooth_idf=False, max_features=max_features)
    tfidf = tfidf_vec.fit_transform(docs)
    idf_dict = dict(zip(tfidf_vec.get_feature_names(), tfidf_vec.idf_))
    features = []
    idfs = []
    for item in idf_dict.items():
        features.append(item[0].encode('utf-8'))
        idfs.append(item[1])
    idf_df = pd.DataFrame({'feature':features, 'idf':idfs}, index=None)
    idf_df.to_csv(idf_save_path, index=False, header=False, sep=' ')


def coll_cut_chnal(chname, num, save_dir, cut_save_file):
    try:
        save_path = os.path.join(save_dir, chname+'_raw.csv')
        coll_chnal(chname, num, True, save_path)
        print '-------{} coll finish!'.format(chname)
        raw_df = pd.read_csv(save_path)
        docs_series = raw_df['doc']
        docs_series = docs_series.apply(cut_pos_ltp, (True, allow_pos_ltp, False))
        raw_df['doc'] = docs_series
        #raw_df = raw_df.dropna()
        raw_df.to_csv(cut_save_file, index=False)
        print '**************{} cut finished! '.format(chname)
    except:
        traceback.print_exc()


def coll_cut_extract_multiprocess(chnl_num_dict,
                                  save_dir,
                                  idf_save_path,
                                  topK=100,
                                  max_percent=0.3,
                                  to_csv_file=True):
    from multiprocessing import Pool
    pool = Pool(30)
    chnl_cut_file = []
    for item in chnl_num_dict.items():
        chnl = item[0]
        num = item[1]
        cut_save_path = os.path.join(save_dir, chnl+'_cut.csv')
        chnl_cut_file.append(cut_save_path)
        pool.apply_async(coll_cut_chnal, args=(chnl, num, save_dir, cut_save_path))
        #coll_cut_chnal(chnl, num, save_dir)
    pool.close()
    pool.join()

    data_cut_path = os.path.join(save_dir, 'data_cut.csv')
    join_csv(chnl_cut_file, data_cut_path, columns=('chnl', 'nid', 'doc'))
    cut_df = pd.read_csv(data_cut_path)
    cut_df = cut_df.dropna()
    cut_docs = cut_df['doc']
    get_idf_file(cut_docs, idf_save_path, 5000000)
    extract_docs = extract_keywords(idf_save_path, cut_docs.tolist(), topK, max_percent)
    cut_df['doc'] = extract_docs
    if to_csv_file:
        cut_df.to_csv(os.path.join(save_dir, 'cut_extract.csv'), index=False)


#########################  提供多进程版本结束  ####################################


################################################################################
#@brief: 根据频道过滤,只保留特定的频道
#@input:  nids
#         cnames
#@output: nids of cnames
################################################################################
knbc_sql = "select nid from info_news ni inner join channellist_v2 cl " \
           "on ni.chid=cl.id " \
           "where nid in %s and cl.cname in %s "
def keep_nids_based_cnames(nids_tuple, cname_tuple):
    conn, cursor = get_postgredb_query()
    cursor.execute(knbc_sql, (nids_tuple, cname_tuple))
    rows = cursor.fetchall()
    keep_nids = []
    for r in rows:
        keep_nids.append(r[0])
    return keep_nids


################################################################################
#@brief: 获取新闻的句子
#@input: set of nids
#@output:
#      nid_para_sentences_dict  ---  nid : paragraph index : list of sentence
#      nid_para_links_dict      ---  nid : paragraph index : list of links
#      nid_pname_dict           ---  nid : pname
################################################################################
get_sent_sql = "select nl.nid, nl.title, ni.content, nl.state, nl.pname from info_news ni " \
               "inner join newslist_v2 nl on ni.nid=nl.nid " \
               "inner join channellist_v2 cl on nl.chid=cl.id " \
               "where ni.nid in %s"
def get_nids_sentences(nid_set):
    nid_tuple = tuple(nid_set)
    conn, cursor = get_postgredb_query()
    cursor.execute(get_sent_sql, (nid_tuple, ))
    rows = cursor.fetchall()
    nid_sentences_dict = {}
    nid_para_links_dict = {}
    nid_pname_dict = {}
    for r in rows:
        if r[3] != 0:  # 已被下线
            continue
        nid = r[0]
        nid_sentences_dict[nid] = {}
        nid_para_links_dict[nid] = {}
        nid_pname_dict[nid] = r[4]
        content_list = r[2]
        pi = 0
        for content in content_list:
            if "txt" in content.keys():
                pi += 1  # paragraph index
                soup = BeautifulSoup(content['txt'], 'lxml')
                pi_sents = []
                for link in soup.find_all('a'):
                    pi_sents.append(link)  # 记录每一段的链接
                nid_sentences_dict[nid][pi] = Cut(soup.text)
                nid_para_links_dict[nid][pi] = pi_sents
    conn.close()
    return nid_sentences_dict, nid_para_links_dict, nid_pname_dict




