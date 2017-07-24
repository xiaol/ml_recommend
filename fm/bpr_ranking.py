# coding: utf-8

from sklearn.cross_validation import train_test_split
from fastFM import bpr
from sklearn.metrics import mean_squared_error
from model import construct_feature_matrix

def train_model(x_train, y_train, n_iter,
                init_stdev=0.1, rank=2, l2_reg_w=0.1, l2_reg_V=0.5):
    fm = bpr.FMRecommender(n_iter=n_iter, init_stdev=init_stdev, rank=rank, l2_reg_w=l2_reg_w, l2_reg_V=l2_reg_V)
    fm.fit(x_train, y_train)
    return fm


def train(n_iter=1000, time_interval='1 hour', user=[],
          init_stdev=0.1, rank=2, l2_reg_w=0.1, l2_reg_V=0.5,
          test_size=0.33, random_state=42):
    # TODO y need to be a pair to rank the sample row
    X, y, user_extractor, item_extractor = construct_feature_matrix(5000,user=user, time_interval=time_interval)
    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

    als_fm = train_model(x_train, y_train, n_iter, init_stdev=init_stdev, rank=rank, l2_reg_w=l2_reg_w, l2_reg_V=l2_reg_V)
    y_pred = als_fm.predict(x_test)

    #print y_pred
    print 'mse:', mean_squared_error(y_test, y_pred)
    return als_fm, (x_train, y_train, x_test, y_test), user_extractor,item_extractor


if __name__ == '__main__':
    # This sets up a small test dataset.
    # X, y, _ = make_user_item_regression(label_stdev=.4)
    X, y, user_extractor, item_extractor = construct_feature_matrix(5000, user=[33658617])
    X_train_1, X_test_1, y_train_1, y_test_1 = train_test_split(X, y)

    als_fm = train_model(X_train_1, y_train_1, 1000)
    y_pred = als_fm.predict(X_test_1)

    print y_pred

    print 'mse:', mean_squared_error(y_test_1, y_pred)
    # mse: 0.0625933908868 baseline
    # add user topic feature without regularization, mse: 0.508930280732 , with reg  mse: 0.121558287279
    # add item topic feature with reg,   mse: 0.0582484478577