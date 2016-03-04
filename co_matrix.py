import sys
import ast
import re

from pyspark import SparkContext

def flat_Map(document):
    return document[1].rstrip( ).split(' ')

def flat_Line(line):
    line_split = line.split( )
    line_split = line_split[1:]
    n = len(line_split)
    tuples = []
    for i in range(0, n):
        A = line_split[i].split(',')
        j = 0
        while(j < n):
            B = line_split[j].split(',')
            key = (A[0], B[0])
            value = (int(A[1]) + int(B[1])) / 2
            j = j + 1
            tuples.append((key,value))
    return tuples

def map(tuple):
    key = tuple[0][0]
    value = tuple[0][1] + ',' + str(tuple[1])
    return (key, value)

def reduce_value(value_a, value_b):
    return value_a + '\t' + value_b

def reduce(value_a, value_b):
    return int(value_a) + int(value_b)

def co_matrix(file_name, output="co_matrix.out"):
    sc = SparkContext("local[8]", "UserArtistMatrix")
    """ Reads in a sequence file FILE_NAME to be manipulated """
    file = sc.sequenceFile(file_name)

    """
    - flatMap takes in a function that will take one input and outputs 0 or more
      items
    - map takes in a function that will take one input and outputs a single item
    - reduceByKey takes in a function, groups the dataset by keys and aggregates
      the values of each key
    """
    counts = file.flatMap(flat_Map) \
                 .flatMap(flat_Line) \
                 .reduceByKey(reduce) \
                 .map(map) \
                 .reduceByKey(reduce_value) \
                 .sortByKey()

    """ Takes the dataset stored in counts and writes everything out to OUTPUT """
    counts.map(lambda x: x[0] + '\t' + x[1]).coalesce(1).saveAsTextFile(output)

""" Do not worry about this """
if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        co_matrix(argv[1])
    else:
        co_matrix(argv[1], argv[2])
