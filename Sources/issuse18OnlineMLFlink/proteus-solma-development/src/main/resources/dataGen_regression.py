from time import time
from scipy import sparse
from scipy import linalg

from sklearn.datasets.samples_generator import make_regression
from sklearn.linear_model import Lasso


from sklearn.svm import LinearSVC
from sklearn.datasets import make_classification
from sklearn.svm import LinearSVC
from sklearn.datasets import make_classification
#nSamples = 160359*250
nSamples = 20000

X, y ,cof= make_regression(n_samples=nSamples, n_features=7, random_state=10, noise=0.01, coef=True)
print("true cof : {}".format(cof))
#X, y = make_classification(n_samples=16, n_features=7, random_state=10, flip_y=0.03)
trainLen=int(len(y)*0.7)
with open('onlineML_regression.csv',"w") as f:
    for x,sy in zip(X[:trainLen],y[:trainLen]):
        line = "LabeledVector("+str(sy)+", DenseVector("+", ".join(str(s) for s in x)+"))"
        f.write(line+"\n")
    for x,sy in zip(X[trainLen:],y[:trainLen]):
        sy=-9.0
        line = "LabeledVector("+str(sy)+", DenseVector("+", ".join(str(s) for s in x)+"))"
        f.write(line+"\n")
