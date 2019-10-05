import os
import sys

import matplotlib.pyplot as plt
import networkx as nx
import time
import json

# filtering phase phase Remove unimportant regions of the graph
# Trivially separable from the rest of the graph
# Do not participate in overlapping clustering
# Our filtering procedure
# Remove all single-edge biconnected components (remain connected after
# removing any vertex and its adjacent edges)
# Compute the largest connected component
# from sse.Graclus_centers import Graclus_centers
from Graclus_centers import Graclus_centers
from graph_building import file_graph_building
from seed_set_expansion import seed_set_expansion, color_building_list

mon_fichier = open("logs.txt", "w")  # Argh j'ai tout écrasé !


# data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'graph.txt')
#
# if __name__ == "__main__":
def filtering_phase(G):
    liste = []
    nb_node = G.nodes(data=False)

    for node in nb_node:
        if len(list(G.neighbors(node))) <= 1:
            liste.append(node)

    if (len(liste) != 0):
        G.remove_node(liste[0])

        return filtering_phase(G)
    return G


G = file_graph_building(sys.argv[1])
# G = file_graph_building(data_path)

t = time.time()
print("filtering_phase processing....")
G = filtering_phase(G)
print(len(G.nodes()))
mon_fichier.write("Filtering phase in :" + repr(time.time() - t) + "\n")
print("filtering_phase done!")

t = time.time()
print("seeding phase")
seeds = Graclus_centers(G)
mon_fichier.write("seeding phase in :" + repr(time.time() - t) + "\n")
print("seeding phase done!")

t = time.time()
print("seed set expansion phase")
expansion = seed_set_expansion(G, seeds)
mon_fichier.write("seed set expansion  phase in :" + repr(time.time() - t) + "\n")
# save expansion result
output_file = os.path.join(os.path.dirname(__file__), 'expansion.txt')
if os.path.exists(output_file):
    os.remove(output_file)
with open(output_file, 'w') as f:
    f.write(json.dumps(expansion))
print("seedingset expansion phase done!")

print("Graph building with coloring community")
values = color_building_list(G, expansion)
nx.draw_spring(G, cmap=plt.get_cmap('jet'), node_color=values, node_size=30, with_labels=False)
mon_fichier.write("building graph with community colors  in :" + repr(time.time() - t) + "\n")
print("building graph  done!")
mon_fichier.close()
plt.show()
