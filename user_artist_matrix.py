import sys

from pyspark import SparkContext

def flat_Map(document):
    return document[1].rstrip( ).split(' ')

def map(line):
    line_split = line.split('\t')
    key = line_split[0]
    value = line_split[1] + ',' + line_split[2]
    return (key, value)  

def reduce(value_a, value_b):
    return value_a + '\t' + value_b

def user_artist_matrix(file_name, output="user_artist_matrix.out"):
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
                 .map(map) \
                 .reduceByKey(reduce) \
                 .sortByKey(keyfunc=lambda k: int(k))

    """ Takes the dataset stored in counts and writes everything out to OUTPUT """
    counts.map(lambda x:x[0]+'\t'+ x[1]).coalesce(1).saveAsTextFile(output)

""" Do not worry about this """
if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        user_artist_matrix(argv[1])
    else:
        user_artist_matrix(argv[1], argv[2])
