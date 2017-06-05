# -*- coding: utf-8 -*-
# @Time    : 17/6/5 上午11:51
# @Author  : liulei
# @Brief   : linucb feature计算服务
# @File    : linucb_service.py
# @Software: PyCharm Community Edition
import sys

port = sys.argv[1]

if port == 9959:
    from linUCB import user_feature
    user_feature.get_active_user_info(3, 20)
