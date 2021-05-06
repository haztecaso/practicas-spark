#!/usr/bin/env python3
from pyspark import SparkContext

def get_edges(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2


def get_rdd_distict_edges(rdd):
    return rdd\
        .map(get_edges)\
        .filter(lambda x: x is not None)\
        .distinct()


def adjacents(rdd):
    nodes = get_rdd_distict_edges(rdd)
    adj = nodes.groupByKey()
    return adj


def main(filename):
    sc = SparkContext()
    adj = adjacents(sc.textFile(filename)).collect()
    for node in adj:
        print(node[0], list(node[1]))


if __name__ == '__main__':
    from sys import argv
    if len(argv)>1:
        main(argv[1])
    else:
        main('grafo.txt')

