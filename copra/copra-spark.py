# python v2.7

import re
import os
import random
from pyspark import SparkConf, SparkContext
import json
import sys
import re


# initialize spark
conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

try:
  filename = sys.argv[1]
except:
  filename = 'graph.txt'

graph_file = os.path.join(os.path.dirname(__file__), '..', 'data', filename)

lines = sc.textFile(graph_file);

def read_line(l):
  (src, dst) = re.split(r'[^\w]+', l)
  return [(src, dst), (dst, src)]

print('#### start reading file')
print('\n')

edges = lines.flatMap(read_line)

print('#### finish reading file')
print('\n')

# nodes is in (node, neighbours) key pair
nodes = edges.groupByKey();

# label_nodes is in (node, [
#   {
#     ngbs: list of ngbs 
#     labels: list of labels
#   },
#   {
#     ...
#   }
# ]) key pair

# initialize the each node's label with itself
nodes = nodes.map(lambda (v, ngbs): (v, {
  'ngbs': list(ngbs),
  'labels': [(v, 1)]
}))

def normalize_labels(labels_map):
  new_labels_map = {}  
  sum = reduce(lambda acc, val: acc + val, labels_map.values())
  for label, value in labels_map.items():
      new_labels_map[label] = value / float(sum)
  return new_labels_map

def filter_labels(labels_map, threadhold):
  new_labels_map = {}
  for label, value in labels_map.items():
      if value >= threadhold:
          new_labels_map[label] = value
  return new_labels_map

def pick_max_label(labels_map):
  label_list = [(label, value) for label, value in labels_map.items()]

  # find the max value among label_list
  sorted_label_list = sorted(label_list, key=lambda item: item[1], reverse=True)
  max_value = sorted_label_list[0][1]

  # all labels with max value
  max_labels = list(filter(lambda item: item[1] == max_value, sorted_label_list))

  # randomly pick one with max value
  random_item = random.choice(max_labels)
  return { random_item[0]: random_item[1] }

def propagate_new_labels(info, threadhold):
  ngbs = info['ngbs'] 

  labels_map = {}
  for (ngb_v, ngb_labels) in ngbs:
    for (label, value) in ngb_labels:
      if label in labels_map:
        labels_map[label] = labels_map[label] + value
      else:
        labels_map[label] = value

  normalized_labels_map = normalize_labels(labels_map)
  filtered_labels_map = filter_labels(normalized_labels_map, threadhold)

  # check if filtered_labels_map still have labels inside
  if len(filtered_labels_map.keys()) == 0:
    # randonly pick the label with the max value
    filtered_labels_map = pick_max_label(normalized_labels_map)

  new_labels_map = normalize_labels(filtered_labels_map) 
  return [(label, value) for (label, value) in new_labels_map.items()]


def get_labels_set_size(nodes):
  all_labels_list = nodes.mapValues(lambda info: info['labels']).flatMap(lambda (v, labels): labels).collect()
  return sc.parallelize(all_labels_list).groupByKey().count()

def get_communities(nodes):
  all_communities_list = nodes.mapValues(lambda info: info['labels']).flatMap(lambda (v, labels): map(lambda (label, _): (label, v), labels)).collect()
  return sc.parallelize(all_communities_list).groupByKey().mapValues(list)

def copra(nodes, k=2):
  iteration = 0

  print('start calculating labels set size')
  old_labels_set_size = get_labels_set_size(nodes)
  print('finish calculating labels set size')
  print('label size is ' + str(old_labels_set_size))
  print('\n')

  while True:
    iteration += 1
    print('\n')
    print('#### Iteration ' + str(iteration))


    print('start converting to nodes map')
    nodes_map = nodes.collectAsMap()
    print('finish converting to nodes map')
    print('\n')

    print('start mapping neighbours nodes')
    nodes = nodes.mapValues(lambda info: {
      'ngbs': map(lambda v: (v, nodes_map[v]['labels']), info['ngbs']),
      'labels': info['labels']
    })
    print('finish mapping neighbours nodes')
    print('\n')

    print('start updating labels')
    # update the labels for each node
    nodes = nodes.mapValues(lambda info: {
      'ngbs': map(lambda (v, _): v, info['ngbs']),
      'labels': propagate_new_labels(info, 1/float(k)),
    })
    print('finish updating labels')
    print('\n')

    print('start calculating labels set size')
    new_labels_set_size = get_labels_set_size(nodes)
    print('finish calculating labels set size')
    print('label size is ' + str(new_labels_set_size))
    print('\n')

    print('\n')

    if new_labels_set_size == old_labels_set_size:
      break
    else:
      old_labels_set_size = new_labels_set_size

  print('start getting communities')
  print('\n')
  return get_communities(nodes).collectAsMap()

try: 
  k = sys.argv[2]
except:
  k = 2

print('#### start the copra process with communities number ' + str(k))
communities = copra(nodes, k);

def save_output(result, filename):
    # save expansion result
    output_file = os.path.join(os.path.dirname(__file__), 'output', filename)
    if os.path.exists(output_file):
        os.remove(output_file)
    with open(output_file, 'w') as f:
        f.write(json.dumps(result))

print('\n')
print('#### save the output')
save_output(communities, os.path.basename(graph_file))

# stop spark
sc.stop()
