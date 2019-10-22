import re
import sys
from pyspark import SparkConf, SparkContext
import os
import glob
import math
import argparse
import random

conf = SparkConf()
sc = SparkContext(conf=conf)

def read_graph_from_file(path):

    edge = sc.textFile(graph_file).flatMap(lambda x: re.split('\n',x))
    node = sc.textFile(graph_file).flatMap(lambda x: re.split(r"[ \n]",x))

    edgel = edge.map(lambda x: (re.split(' ',x)[0],re.split(' ',x)[1]))
    edger = edge.map(lambda x: (re.split(' ',x)[1],re.split(' ',x)[0]))
    edge = edgel.union(edger).distinct()
    edge = edge.groupByKey().mapValues(list) # List speaker for each listener, (listener, (speaker1, speaker2, ...))

    node = node.distinct().map(lambda x: (x, x)) # Give each node initial community lable, (node, tag)

    return edge, node

def slpa(edge, node, percentage, iteration):
    for i in range(1,iteration):
        rnode = node.map(lambda x: (x[0],x[1][random.randint(0,len(x[1])-1)])) 
        itag = edge.flatMapValues(f) 
        itag = itag.map(lambda x: (x[1],x[0])) 
        itag = itag.join(rnode) # (speaker, (listener, speaker tag))
        itag = itag.map(lambda x: (x[1],1)) # ((listener, speaker tag),1)
        itag = itag.groupByKey().mapValues(len) # ((listener, speaker tag),count)
        itag = itag.map(lambda x: (x[0][0],(x[0][1],x[1]))) # (listener, (speaker tag,count))
        itag = itag.reduceByKey(lambda n1,n2: (n1[0], n1[1]) if n1[1]>=n2[1] else (n2[0],n2[1]))
        itag = itag.map(lambda x: (x[0],x[1][0]))
        node = node.join(itag)
        if i > 1:
            node = node.map(lambda x: (x[0],(x[1][0]+(x[1][1],))))# (listener, (tag1, tag2))
    lsedget = node.flatMapValues(f)
    #writetxt(lsedge.collect(),'lswithoutfilter.txt')
    lsedge = lsedget.map(lambda x: (x,1))
    scount = lsedget.map(lambda x: (x[0],1))
    scount = scount.groupByKey().mapValues(len)
    lsedge = lsedge.reduceByKey(lambda n1,n2: n1+n2)
    lsedge = lsedge.map(lambda x: (x[0][0],(x[0][1],x[1]))) # (listener, (tag, tag count))
    #writetxt(lsedge.collect(),'lswithoutfilter.txt')
    #writetxt(scount.collect(),'scount.txt')
    lsedge = lsedge.join(scount) # (listener, ((tag, tag count),total tag))
    #writetxt(lsedge.collect(),'lsnumber.txt')
    lsedge = lsedge.map(lambda x: (x[0],(x[1][0][0],float(x[1][0][1])/float(x[1][1])))) # (listener, (tag, tag count/total tag))
    lsedge = lsedge.filter(lambda x: x[1][1]>=percentage)
    lsedge = lsedge.map(lambda x: (x[0],x[1][0]))
    #writetxt(lsedge.collect(),'lswithoutfilter.txt')
    node = lsedge.groupByKey().mapValues(list)
    return node

def f(x): return x

def writetxt(lst,name):
    with open(name,'w') as f:
        for item in lst:
            f.write(str(item))
            f.write('\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--percentage', help='percentage of community popularity', default=0.1)
    parser.add_argument('--iteration', help='number of iteration', default=5)
    parser.add_argument('--filename', help='file in data folder', default='graph.txt')
    args = parser.parse_args()

    graph_file = os.path.join(os.path.dirname(__file__), '..', 'data', args.filename)

    edge, node = read_graph_from_file(graph_file)
    print('Graph loaded\n')

    percentage = args.percentage
    iteration = args.iteration

    node = slpa(edge, node, percentage, iteration)
    community = node.flatMapValues(f)
    community = community.map(lambda x: (x[1],x[0]))
    community = community.groupByKey().mapValues(list)
    writetxt(community.collect(),'final.txt')

    print(community.collect())
