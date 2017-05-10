# -*- coding: utf-8 -*-
# @Time    : 17/3/17 下午5:31
# @Author  : liulei
# @Brief   : 包含一些公共的函数
# @File    : common.py
# @Software: PyCharm Community Edition

################################################################################
#@brief :获取 top K
#@solution: 类似快排的思路.    时间复杂度为O(n)
################################################################################
def get_topK(data_list, k):
    if k > len(data_list):
        return data_list
    pivot =  data_list[-1]
    right = [pivot] + [x for x in data_list if x > pivot]
    rlen = len(right)
    if rlen == k:
        return right
    elif rlen < k:
        left = [x for x in data_list if x < pivot]
        return get_topK(left, k - rlen) + right
    else:
        return get_topK(right, k)


