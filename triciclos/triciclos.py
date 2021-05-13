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


def get_distict_edges(rdd):
    return rdd\
        .map(get_edges)\
        .filter(lambda x: x is not None)\
        .distinct()


def get_node_adjs(rdd):
    return get_distict_edges(rdd)\
                .groupByKey()\


def tag(node_adjs): # Función interativa, no perezosa
    node = node_adjs[0]
    adjs = list(node_adjs[1])
    adjs.sort()
    result = [((node, x), 'exists') for x in adjs]
    for i in range(len(adjs)):
        for j in range(i, len(adjs)):
            result.append(((adjs[i], adjs[j]), ('pending', node)))
    return result


def tricycles(tags):
    return tags\
            .groupByKey()\
            .filter(lambda x: len(x[1]) > 1 and 'exists' in x[1])\
            .flatMap(
                lambda line: map(
                    lambda x: (x[1], line[0][0], line[0][1]),
                    filter(lambda x: not x == 'exists', line[1])
                    )
                )


def process_data(data):
    node_adjs = get_node_adjs(data)
    tags = node_adjs.flatMap(tag)
    return tricycles(tags)


def mixed(sc, rdds):
    for rdd in rdds:
        rdd.cache()
    data = sc.union(rdds)
    for ciclo in process_data(data).collect():
        print(ciclo)


def independent(sc, rdds, files):
    for i in range(len(rdds)):
        print(f"file: {files[i]}")
        for e in process_data(rdds[i]).collect():
            print(e)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Calcular los 3-ciclos de un grafo")
    parser.add_argument("ficheros", help="Archivos .txt con listas de aristas", nargs = '+')
    parser.add_argument("--indep", help="Calcular los 3-ciclos de cada fichero de forma independiente", action='store_true' )
    args = parser.parse_args()
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        rdds = [sc.textFile(f) for f in args.ficheros]
        if args.indep:
            independent(sc, rdds, args.ficheros)
        else:
            mixed(sc, rdds)
