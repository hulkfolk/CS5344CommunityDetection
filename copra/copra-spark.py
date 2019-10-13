# python v2.7

import re
import os
from pyspark import SparkConf, SparkContext


# initialize spark
conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile('../data/graph.txt');

def read_line(l):
  (src, dst) = ' '.split(l);
  return [(src, dst), (dst, src)]

links = lines.flatMap(read_line)



# read all files as one rdd from datafiles
def read_file(f):
  return sc.textFile('datafiles/' + f).map(lambda l: (l, f))

files = [read_file(f) for f in os.listdir('datafiles')]
lines = sc.union(files)

# split each line into words with each word as (word, file) pair
def read_line((l, f)):
  words = re.split(r'[^\w|\']+', l)
  filtered_words = [x for x in words if str(x) is not ""]
  return map(lambda w: (w.lower(), f), filtered_words)

word_pairs = lines.flatMap(read_line)

#  word_pairs is (word + ' ' + file, (word, file, 1)) pair 
word_pairs = word_pairs.map(lambda (w, f): (w + ' ' + f, (w, f, 1)))

# count words from same file with same key word + ' ' + file
word_counts = word_pairs.reduceByKey(lambda (w1, f1, n1), (w2, f2, n2): (w1, f1, n1 + n2))

# word_counts is (word, (file, count)) pair
word_counts = word_counts.map(lambda (_, (w, f, count)): (w, (f, count)))

# filter out stopwords after aggregtaion of words 
# filter out common words appearing in all files by check the length of occurances equal to the length of total files
stopwords = sc.textFile('stopwords.txt').collect()
files_len = len(files)
word_counts = word_counts.groupByKey().filter(lambda (w, occurances): str(w) not in stopwords and len(occurances) == files_len).mapValues(list)

# get the least count for each common word
word_counts = word_counts.map(lambda (w, occurances): (w, sorted(map(lambda (f, count): count, occurances))))

# sort the words in descending order by counts
word_counts = word_counts.sortBy(lambda (w, counts): -1 * counts[0])

with open('output_a.txt', 'w') as f:
  for (w, counts) in word_counts.collect():
    f.write(str(w) + " " + str(counts[0]))
    f.write('\n')

# stop spark
sc.stop()
