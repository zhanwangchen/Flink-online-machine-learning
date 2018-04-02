from time import time
from scipy import sparse
from scipy import linalg

from sklearn.datasets.samples_generator import make_regression
from sklearn.linear_model import Lasso

from sklearn.svm import LinearSVC
from sklearn.datasets import make_classification
from sklearn.svm import LinearSVC
from sklearn.datasets import make_classification
#flip_y=0.00001
X, y = make_classification(n_samples=200000, n_features=7, random_state=10, )
# X, y = make_classification(n_samples=16, n_features=7, random_state=10, flip_y=0.03)
trainLen = int(len(y) * 0.85)
with open('onlineMLBig3.csv', "w") as f:
    for x, sy in zip(X[:trainLen], y[:trainLen]):
        line = str(sy) + "," + ", ".join(map(str, list(x)))

        f.write(line + "\n")
    for x, sy in zip(X[trainLen:], y[:trainLen]):
        sy = -9.0
        line = str(sy) + "," + ", ".join(map(str, list(x)))
        f.write(line + "\n")

with open('onlineMLBig3_true.csv', "w") as f:
    for x, sy in zip(X[trainLen:], y[trainLen:]):
        #line = str(sy) + "," + ", ".join(map(str, list(x)))
        line = "LabeledVector("+str(sy)+", DenseVector("+", ".join(str(s) for s in x)+"))"

        f.write(line + "\n")
