# -*- coding: utf-8 -*-


import numpy as np
from memory_profiler import profile
from sklearn.neural_network import BernoulliRBM
from sklearn import linear_model, datasets, metrics
from sklearn.neural_network import BernoulliRBM
from sklearn.pipeline import Pipeline
import matplotlib.pyplot as plt

@profile
def test_numpy_array():
    a = [1]*10000000
    ar = np.array(a, copy=False)


if __name__ == '__main__':
    # test_numpy_array()
    #X = np.array([[0, 0, 0], [0, 1, 1], [1, 0, 1], [1, 1, 1]])
    training_data = np.array(
        [[1, 1, 1, 0, 0, 0], [1, 0, 1, 0, 0, 0], [1, 1, 1, 0, 0, 0], [0, 0, 1, 1, 1, 0], [0, 0, 1, 1, 0, 0],
         [0, 0, 1, 1, 1, 0]])  # A 6x6 matrix where each row is a training example and each column is a visible unit.
    X = np.array([0, 0, 0])
    rbm = BernoulliRBM(random_state=30, verbose=True,
                       n_components=100, n_iter=1000, learning_rate=0.06,)

    logistic = linear_model.LogisticRegression()

    classifier = Pipeline(steps=[('rbm', rbm), ('logistic', logistic)])

    classifier.fit(training_data, y=[0,0,0,1,1,1])

    print classifier.predict([1,0,1,0,1,0])

    plt.figure(figsize=(4.2, 4))
    for i, comp in enumerate(rbm.components_):
        plt.subplot(10, 10, i + 1)
        # plt.imshow(comp, cmap=plt.cm.gray_r,
        #           interpolation='nearest')
        plt.xticks(())
        plt.yticks(())
    plt.suptitle('100 components extracted by RBM', fontsize=16)
    plt.subplots_adjust(0.08, 0.02, 0.92, 0.85, 0.08, 0.23)

    plt.show()

print '--------hold on---------------'
