import sys
import community
import time
from Graclus_centers import get_clusters_node, Graclus_centers
from graph_building import file_graph_building

# filtering phase phase Remove unimportant regions of the graph
# Trivially separable from the rest of the graph
# Do not participate in overlapping clustering
# Our filtering procedure
# Remove all single-edge biconnected components (remain connected after
# removing any vertex and its adjacent edges)
# Compute the largest connected component
from sse.deprecated.seed_set_expansion import seed_set_expansion

mon_fichier = open("com1.txt", "w")  # Argh j'ai tout écrasé !


def filtering_phase(G):
    liste = []
    nb_node = G.nodes_iter(data=False)

    for node in nb_node:
        if len(G.neighbors(node)) <= 1:
            liste.append(node)

    if (len(liste) != 0):
        G.remove_node(liste[0])

        return filtering_phase(G)
    return G


G = file_graph_building(sys.argv[1])

t = time.time()
print("filtering_phase processing....")
G = filtering_phase(G)
print(len(G.nodes()))
print("filtering_phase done!")

t = time.time()
print("seeding phase")
seeds = Graclus_centers(G)
print("seeding phase done!")

t = time.time()
print("seed set expansion phase")
expansion = seed_set_expansion(G, seeds)
print("seedingset expansion phase done!")
print("Graph building with coloring community")

for valeur in expansion.values():
    string = ""
    for element in valeur:
        string = string + str(element) + " "
    mon_fichier.write(string.rstrip())
    mon_fichier.write("\n")

print("building graph  done!")
mon_fichier.close()

mon_fichier = open("com2.txt", "w")  # Argh j'ai tout écrasé !

G = file_graph_building(sys.argv[1])

part = community.best_partition(G)

clusters = []

for val in part.values():
    if val in clusters:
        continue
    else:
        clusters.append(val)
print(clusters)

for cluster in clusters:
    string = ""
    for element in get_clusters_node(part, cluster):
        string = string + str(element) + " "
    mon_fichier.write(string.rstrip())
    mon_fichier.write("\n")

mon_fichier.close()
