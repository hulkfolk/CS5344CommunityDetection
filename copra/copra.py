import networkx as nx
import os
from functools import reduce
import random
import argparse
import json
import sys

def read_graph_from_file(path):
    graph = nx.Graph()
    edges_list = []
    fp = open(path)
    edge = fp.readline().split()
    while edge:
        edges_list.append((edge[0], edge[1]))
        edge = fp.readline().split()
    fp.close()
    graph.add_edges_from(edges_list)

    # add label for each node
    for node, data in graph.nodes(True):
        data[node] = 1

    return graph

def merge_labels(labels, neighbour_labels):
    for label, value in neighbour_labels.items():
        if label in labels:
            labels[label] = labels[label] + neighbour_labels[label]
        else:
            labels[label] = neighbour_labels[label]
    return labels

def normalize(labels):
    sum = reduce(lambda acc, val: acc + val, labels.values())
    for label, value in labels.items():
        labels[label] = value / float(sum)
    return labels

def filter_labels(labels, threadhold):
    new_labels = {}
    for label, value in labels.items():
        if value >= threadhold:
            new_labels[label] = value
    return new_labels

def get_max_label(labels):
    label_list = [(label, value) for label, value in labels.items()]

    # find the max value among label_list
    sorted_label_list = sorted(label_list, key=lambda item: item[1], reverse=True)
    max_value = sorted_label_list[0][1]

    # all labels with max value
    max_labels = list(filter(lambda item: item[1] == max_value, sorted_label_list))

    # randomly pick one with max value
    random_item = random.choice(max_labels)
    return { random_item[0]: random_item[1] }


def get_new_labels(node, graph, k):
    merged_neighbour_labels = {}

    for neighbour in graph.adj[node]:
        neighbour_labels = graph.node[neighbour]
        merged_neighbour_labels = merge_labels(merged_neighbour_labels, neighbour_labels) 

    normalized_labels = normalize(merged_neighbour_labels)
    filtered_labels = filter_labels(normalized_labels, 1 / float(k)) 
    if len(filtered_labels.keys()) == 0:
        # pick the label with the max value
        filtered_labels = get_max_label(normalized_labels)
    new_labels = normalize(filtered_labels)
    return new_labels 

def propagate(graph, k):
    all_new_labels = {}

    # get new labels for each node
    for node in graph.nodes():
        new_labels = get_new_labels(node, graph, k)
        all_new_labels[node] = new_labels
    return all_new_labels


def check_stop_condition(graph, all_new_labels):
    # stop the iteration when the size of communities doesn't change
    old_labels_set = get_labels_set(graph.nodes)
    new_labels_set = get_labels_set(all_new_labels)
    print('old labels set size', len(old_labels_set))
    print('new labels set size', len(new_labels_set))
    print('\n')
    return len(old_labels_set) == len(new_labels_set)


def get_labels_set(iterarble):
    labels_set = set()
    for node in iterarble:
        labels = iterarble[node]
        labels_set.update([label for label in labels.keys()])
    return labels_set


def copra(graph, k):
    iteration = 0
    while True:
        iteration += 1
        print('Iteration ' + str(iteration))
        all_new_labels = propagate(graph, k);

        if check_stop_condition(graph, all_new_labels):
            break
        else:
            # update new labels in the graph
            for node, data in graph.nodes(True):
                new_labels = all_new_labels[node]

                # clear old labels
                data.clear()

                # set new labels
                for label in new_labels.keys():
                    data[label] = new_labels[label]
    
    return get_communities(all_new_labels)

def save_output(result, filename):
    # save expansion result
    output_file = os.path.join(os.path.dirname(__file__), 'output', filename)
    if os.path.exists(output_file):
        os.remove(output_file)
    with open(output_file, 'w') as f:
        f.write(json.dumps(result))


def get_communities(all_new_labels):
    communities = {}
    for node, labels in all_new_labels.items():
        for label in labels.keys():
            if label in communities:
                communities[label].append(node)
            else:
                communities[label] = [node]
    return communities


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--communities', help='number of communities', default=2)
    parser.add_argument('--filename', help='file in data folder', default='graph.txt')
    args = parser.parse_args()

    graph_file = os.path.join(os.path.dirname(__file__), '..', 'data', args.filename)

    graph = read_graph_from_file(graph_file)
    print("Graph Loaded\n")

    # number of communities
    k = args.communities

    print('Start the COPRA process with k = ' + str(k) + '\n')
    communities = copra(graph, k)

    save_output(communities, os.path.basename(graph_file))
    print('Finish the COPRA process\n')
