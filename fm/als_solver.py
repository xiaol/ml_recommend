# -*- coding: utf-8 -*-


from fastFM.datasets import make_user_item_regression
from sklearn.cross_validation import train_test_split
from fastFM import als
from sklearn.metrics import mean_squared_error
from model import construct_feature_matrix

# This sets up a small test dataset.
#X, y, _ = make_user_item_regression(label_stdev=.4)
X, y = construct_feature_matrix(5000)
X_train, X_test, y_train, y_test = train_test_split(X, y)

fm = als.FMRegression(n_iter=1000, init_stdev=0.1, rank=2, l2_reg_w=0.1, l2_reg_V=0.5)
fm.fit(X_train, y_train)
y_pred = fm.predict(X_test)

print y_pred

print 'mse:', mean_squared_error(y_test, y_pred)

# mse: 0.0625933908868 baseline
# add user topic feature without regularization, mse: 0.508930280732 , with reg  mse: 0.121558287279
# add item topic feature with reg,   mse: 0.0582484478577


