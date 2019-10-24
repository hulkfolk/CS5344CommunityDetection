from __future__ import division

import itertools
import json
import os

from community import community_louvain
import networkx as nx
from networkx.algorithms.community import girvan_newman, greedy_modularity_communities

sigma = 1


def minimum_of_float_list(liste):
    minimum = 0
    for i in liste:
        print(i)
        if float(i) < minimum:
            minimum = float(i)
            print(minimum)
    return minimum


# get cluster in a graph
def get_community(G, part, nb_cluster):
    # part = community_louvain.best_partition(G)
    liste = []
    for node, cluster in part.items():
        if cluster == nb_cluster:
            liste.append(node)
    return G.subgraph(liste)


def get_key_of_value(dict, value):
    keys = []
    for ele in dict.keys():
        if dict[ele] == value:
            keys.append(ele)
    return keys


def get_clusters_node(part, cluster):
    vertex = []
    for node in part.keys():
        if part[node] == cluster:
            vertex.append(node)
    return vertex


def Graclus_centers(G):
    seeds = []
    print(">>>>>>>> compute non-overlapping clusters")
    # dendrogram = community_louvain.generate_dendrogram(G)
    # with open(os.path.join(os.path.dirname(__file__), 'output', "intermediate.txt")) as f:
    #     lines = f.readlines()
    #
    # d = json.loads(lines[0])
    # dendrogram = []
    # for item in d:
    #     tmp = dict((int(k), v) for k, v in item.items())
    #     dendrogram.append(tmp)
    #
    # part = community_louvain.partition_at_level(dendrogram, len(dendrogram)-4)
    #
    # clusters = {}
    # for k, v in part.items():
    #     if v in clusters:
    #         clusters[v].append(k)
    #     else:
    #         clusters[v] = [k]


    clusters = {}
    count = 1
    k = 3
    comp = girvan_newman(G)
    c = list(greedy_modularity_communities(G))
    for parts in itertools.islice(comp, k):
        print(parts)
        count += 1
        if count == k:
            i = 0
            for cluster in parts:
                clusters[i] = [i for i in cluster]
                i += 1

    print(">>>>>>>>>> clusters len: " + str(len(clusters)))

    for cluster in clusters.keys():
        distances = {}
        # subGraph = get_community(G, part, cluster)
        sub_graph = G.subgraph(clusters[cluster])
        edges = sub_graph.number_of_edges()
        print(edges)
        degrees = sum(dict(G.degree(sub_graph.nodes())).values())
        # print("degrees %d  " % degrees)
        # print("cluster %d  " % cluster)
        for node in clusters[cluster]:
            if sub_graph.has_node(node):
                node_degrees = sub_graph.degree(node)

                node_edges = len(list(G.neighbors(node)))

                if node_degrees != 0 and node_edges != 0:
                    distances.update(
                        {node: (-2 * node_edges / node_degrees * degrees) + (edges / degrees ** 2) + (sigma / node_degrees) - (
                                sigma / degrees)})
        if distances:
            print("cluster %d minimum distance %f" % (cluster, min(distances.values())))

            seeds.append(get_key_of_value(distances, min(distances.values()))[0])
    return seeds
