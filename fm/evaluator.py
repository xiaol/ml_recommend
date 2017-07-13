# -*- coding: utf-8 -*-

from fastFM import als
from fastFM.datasets import make_user_item_regression
from sklearn.metrics import mean_squared_error, r2_score
import numpy as np
from als_solver import train

# X, y, coef = make_user_item_regression(label_stdev=.4)
from sklearn.cross_validation import train_test_split
# X_train, X_test, y_train, y_test = train_test_split(
#  X, y, test_size=0.33, random_state=42)

als_fm, X_Y, u_extractor, i_extractor = train(time_interval='10 minutes', n_iter=100, test_size=0.33, random_state=42,
                    init_stdev=0.1, l2_reg_w=0.2, l2_reg_V=0.5, rank=2)

n_iter = 5
step_size = 1000

#fm = als.FMRegression(n_iter=0, l2_reg_w=0.1, l2_reg_V=0.1, rank=4)
# Allocates and initalizes the model parameter.
#fm.fit(X_train, y_train)

rmse_train = []
rmse_test = []
r2_score_train = []
r2_score_test = []

for i in range(1, n_iter):
    als_fm.fit(X_Y[0], X_Y[1], n_more_iter=step_size)
    y_pred = als_fm.predict(X_Y[2])

    rmse_train.append(mean_squared_error(als_fm.predict(X_Y[0]), X_Y[1]))
    rmse_test.append(mean_squared_error(als_fm.predict(X_Y[2]), X_Y[3]))

    r2_score_train.append(r2_score(als_fm.predict(X_Y[0]), X_Y[1]))
    r2_score_test.append(r2_score(als_fm.predict(X_Y[2]), X_Y[3]))


from matplotlib import pyplot as plt
fig, axes = plt.subplots(ncols=2, figsize=(15, 4))

x = np.arange(1, n_iter) * step_size
with plt.style.context('fivethirtyeight'):
    axes[0].plot(x, rmse_train, label='RMSE-train', color='r', ls="--")
    axes[0].plot(x, rmse_test, label='RMSE-test', color='r')
    axes[1].plot(x, r2_score_train, label='R^2-train', color='b', ls="--")
    axes[1].plot(x, r2_score_test, label='R^2-test', color='b')
axes[0].set_ylabel('RMSE', color='r')
axes[1].set_ylabel('R^2', color='b')
axes[0].legend()
axes[1].legend()

plt.show()
print "---hold-------"
