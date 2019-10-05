import networkx as nx


def difference(S, R):
    print(" prolongation phase processing ...")
    DIF = nx.Graph()
    for edge in S.edges_iter():
        if not R.has_edge(edge[0], edge[1]):
            DIF.add_edge(edge[0], edge[1])

    for edge in DIF.edges_iter():
        R.add_edge(edge[0], edge[1])
    print("after prolongation phase done")
    return R
