#!/usr/bin/env python3
from pyspark import SparkContext
from operator import itemgetter


def get_edges(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1[0] == '"':
        n1 = int(n1[1:-1])
    if n2[0] == '"':
        n2 = int(n2[1:-1])
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2


tupleMorph = lambda tup, x : (tup[0], (tup[1], x))


def get_distict_edges(rdd):
    return rdd\
        .map(lambda x: tupleMorph(get_edges(x[0]),x[1]))\
        .filter(lambda x: x[1] is not None)\
        .distinct()


def get_node_adjs(edges):
    return edges.groupByKey()


def tag(node_adjs): # Función iterativa, no perezosa
    node = node_adjs[0]
    adjs = list(node_adjs[1])
    adjs.sort()
    result = [((node, x), (graph, 'exists')) for (x, graph) in adjs]
    for i in range(len(adjs)):
        for j in range(i, len(adjs)):
            if adjs[j][1] == adjs[i][1] and not adjs[i][0] == adjs[j][0]: #Los nodos aparecen en el mismo grafo
                result.append(((adjs[i][0], adjs[j][0]), (adjs[i][1], ('pending', node))))
    return result


def findExists(tag_list):
    for tag in tag_list:
        if tag[1] == 'exists':
            return True
    return False

def list2dict(lista):
    result = {}
    for key, value in lista:
        if key not in result:
            result[key] = [value]
        else:
            result[key].append(value)
    return result

def construct_tricycles(line):
    edge, tags = line
    return (edge, list2dict(tags))


def tricycles(tags):
    return tags\
            .groupByKey()\
            .filter(lambda x: len(x[1]) > 1 and findExists(x[1]))\
            .map(construct_tricycles)


def process_data(data):
    edges = get_distict_edges(data).cache()
    node_adjs = get_node_adjs(edges).cache()
    tags = node_adjs.flatMap(tag).cache()
    return tricycles(tags)


def independent(sc, rdds, files):
    assert len(rdds) == len(files)
    tagged_rdds = [rdd.map(lambda x: (x,graph)).cache() for rdd, graph in zip(rdds, files)]
    data = sc.union(tagged_rdds)
    for tricycle in process_data(data).collect():
        print(tricycle[0], tricycle[1])


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Calcular los 3-ciclos de un grafo")
    parser.add_argument("ficheros", help="Archivos .txt con listas de aristas", nargs = '+')
    args = parser.parse_args()
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        rdds = [sc.textFile(f) for f in args.ficheros]
        independent(sc, rdds, args.ficheros)
