#!/usr/bin/env python3
from pyspark import SparkContext

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


def get_rdd_distict_edges(rdd):
    return rdd\
        .map(get_edges)\
        .filter(lambda x: x is not None)\
        .distinct()


def adjacents(rdd):
    nodes = get_rdd_distict_edges(rdd).sortBy(lambda tupla: tupla[1])
    adj = nodes.groupByKey().sortByKey()
    return adj


def etiquetar(tupla): # Función interativa, no perezosa
    nodo = tupla[0]
    adyacentes = list(tupla[1])
    result = [((nodo, x), 'existe') for x in adyacentes]
    for i in range(len(adyacentes)):
        for j in range(i, len(adyacentes)):
            result.append(((adyacentes[i], adyacentes[j]), ('pending', nodo)))
    return result


def triciclos(etiquetas):
    return etiquetas\
            .groupByKey()\
            .filter(lambda x: 'existe' in x[1] and len(x[1]) > 1)\
            .flatMap(lambda x: map(lambda y: (y[1], x[0][0], x[0][1]),filter(lambda z: not z == 'existe', x[1])))


def process_data(data):
    adj = adjacents(data)
    etiquetas = adj.flatMap(etiquetar)
    return triciclos(etiquetas)

def ejercicio_2(sc, rdds):
    for rdd in rdds:
        rdd.cache()
    data = sc.union(rdds)
    for ciclo in process_data(data).collect():
        print(ciclo)

def ejercicio_3(sc, rdds, files):
    i = 0
    for rdd in rdds:
        print(f"file: {files[i]}")
        for e in process_data(rdd).collect():
            print(e)
        i += 1


def main(sc, modo, files):
    rdds = [sc.textFile(f) for f in files]
    if int(modo) == 2:
        ejercicio_2(sc, rdds)
    elif int(modo) == 3:
        ejercicio_3(sc, rdds, files)


if __name__ == '__main__':
    from sys import argv
    if len(argv)>1:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, 3, argv[1:])
