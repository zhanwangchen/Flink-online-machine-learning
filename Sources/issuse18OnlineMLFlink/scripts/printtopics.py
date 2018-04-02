#!/usr/bin/python

#
#print out topic distribution learned by LDA

import sys, os, re, random, math, urllib2, time, cPickle
import numpy

#import onlineldavb
global str
def main():
    """
    Displays topics fit by onlineldavb.py. The first column gives the
    (expected) most prominent words in the topics, the second column
    gives their (expected) relative prominence.
    """
    # vocab = str.split(file(sys.argv[1]).read())
    # testlambda = numpy.loadtxt(sys.argv[2])
    vocab = str.split(file("dictnostops.txt").read())
    testlambda = numpy.loadtxt("./out/1lambda.dat")

    for k in range(0, len(testlambda)):
        lambdak = list(testlambda[k, :])
        lambdak = lambdak / sum(lambdak)
        temp = zip(lambdak, range(0, len(lambdak)))
        temp = sorted(temp, key = lambda x: x[0], reverse=True)
        print 'topic %d:' % (k)
        # feel free to change the "53" here to whatever fits your screen nicely.
        #topicToshow 50 53
        topicToshow = 10
        for i in range(0, topicToshow):
            print '%20s  \t---\t  %.4f' % (vocab[temp[i][1]], temp[i][0])
        print


def printsviTopic():
    #resfile = "/media/zhanwang/data/data/lab/flink_t1/Online_FlinkML_Notes/proteus-solma-development/src/main/resources/ASYVI_local_res_nyBig.txt"
    resfile = "/media/zhanwang/data/data/lab/flink_t1/Online_FlinkML_Notes/proteus-solma-development/src/main/resources/ASYVI_local_20news_small2.txt"

    #dictFile =  "/media/zhanwang/data/data/lab/flink_t1/Online_FlinkML_Notes/proteus-solma-development/src/main/resources/nytimes2.vocab.big"
    dictFile = "/media/zhanwang/data/data/lab/flink_t1/Online_FlinkML_Notes/proteus-solma-development/src/main/resources/20news.dict"

    lambdaData = []
    with open(resfile, "r") as f:
        for line in f:
            if line.startswith("lambda<&%$#"):

                lambdaData = line.strip().split(",")
                break

    vacabSize = lambdaData[len(lambdaData)-1]

    #vacabSize=47
    numTopic = lambdaData[-2]
    lambdam  = lambdaData[-4]
    readf = file(dictFile).read()
    vocab = str.split(readf)
    testlambda = numpy.array(lambdam.strip().split(" "), dtype=numpy.double)
    print(len(testlambda))
    testlambda=testlambda.reshape(int(numTopic), int(vacabSize))
    for k in range(0, len(testlambda)):
        lambdak = list(testlambda[k, :])
        lambdak = lambdak / sum(lambdak)
        temp = zip(lambdak, range(0, len(lambdak)))
        temp = sorted(temp, key=lambda x: x[0], reverse=True)
        print 'topic %d:' % (k)
        # feel free to change the "53" here to whatever fits your screen nicely.
        # topicToshow 50 53
        topicToshow = 10
        for i in range(0, 20):
            print '%20s  \t---\t  %.4f' % (vocab[temp[i][1]], temp[i][0])
        print


if __name__ == '__main__':
    #main()
    printsviTopic()
