# -*- coding: utf-8 -*-
# @Time    : 17/7/18 上午11:34
# @Author  : liulei
# @Brief   : 
# @File    : test_kmeans.py
# @Software: PyCharm

import graphlab as gl
import pandas as pd
df = pd.read_csv('d.csv')
docs = df['doc']
print docs
trim_sa = gl.text_analytics.trim_rare_words(docs, threshold=1, to_lower=False)
print docs
docs_trim = gl.text_analytics.count_words(trim_sa)
print docs_trim
#model = gl.kmeans.create(gl.SFrame(docs_trim),
                         #num_clusters=2,
                         #max_iterations=20)
