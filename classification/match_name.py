# -*- coding: utf-8 -*-
# @Time    : 16/8/2 下午5:43
# @Author  : liulei
# @Brief   : 给定文章和文章类别,与list中的名称做匹配,返回匹配的名称
# @File    : match_name.py
# @Software: PyCharm Community Edition
import re
import string

import xlrd
import jieba
import jieba.analyse
#import tornado
#import tornado.gen
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class Names(object):
    def __init__(self, type):
        self.type = type
        self.name_list = []
        self.name_dict = {}
    #从xlsm文件读取所有名称
    def getTypeNamesList(self):
        pass
    def addName(self, name):
        self.name_list.append(name)
    #根据名称列表构造字典
    #字典中每个词的说明由词名、词频和词性（可省）构成
    def createDictForJieba(self):
        self.dict_name = './util/' + self.type + '.dict'
        try:
            with open(self.dict_name, 'wr') as newDict:
                for newName in self.name_list:
                    str = newName + ' 3'  #词频为1, 词性省略
                    newDict.write(str + '\n')
        except IOError as err:
            print('ioerror : %s' + str(err))
    #构造字典,key为关键词,val为包含key的游戏名的列表。
    #例如: {'英雄联盟': ['英雄联盟', '英雄联盟2'}
    def createDictOfName(self):
        names_key = self.name_dict.keys()
        for newName in self.name_list:
            names_key = self.name_dict.keys()
            #如果新名字包含已经统计过的游戏名,则将后者的列表中添加前者
            #例如已有英雄联盟,当统计到英雄联盟2时,在英雄联盟的list中添加英雄联盟2
            for name in names_key:
                if name in newName:
                    self.name_dict[name].append(newName)
            if not self.name_dict.get(newName):
                self.name_dict[newName] = []
                self.name_dict[newName].append(newName)

    #第一层检查:通过书名号查找
    def getNameByQuotationMarks(self, txt):
        pattern = u'《([^》]*)》'
        names = re.findall(pattern, txt)
        for name in names:
            if self.name_dict.get(name):
                return name
        return None


    #获取文章相关的名字
    #@tornado.gen.coroutine
    def getArticalTypeList(self, txt):
        #先用书名号检查
        name = self.getNameByQuotationMarks(txt)
        if name:
            return name

        name = ''
        for item in self.name_dict.keys():
            index = string.find(txt, item)
            if index == -1:
                continue
            name = item
            if len(self.name_dict[item]) > 1:
                name = item
                length = len(item)
                for i in self.name_dict[item]:
                    if string.find(txt, i) == index and len(i) > length:
                        name = i
                        length = len(i)
            return name
        return name

class GameNames(Names):
    def __init__(self):
        super(GameNames, self).__init__('game')
        self.getTypeNamesList()
    def getTypeNamesList(self):
        game_data = xlrd.open_workbook('./util/game_list1.xlsx')
        table = game_data.sheets()[0]
        nrows = table.nrows
        i = 0
        while( i < nrows):
            name = ''.join(table.row_values(i))
            super(GameNames, self).addName(name)
            i += 1
        #构造新字典,供jieba使用
        #super(GameNames, self).createDictForJieba()
        super(GameNames, self).createDictOfName()

gameNames = GameNames()
def NameFactory(type):
    if type == 'game':
        return gameNames
    else:
        pass
