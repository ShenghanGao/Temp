import sys

from pyspark import SparkContext

def flat_user(document):
    return document[1].rstrip( ).split(' ')

def map_user(line):
    line_split = line.split('\t')
    key = line_split[1]
    value = line_split[2]
    return (key, value)  

def flat_matrix(document):
    return document[1].rstrip( ).split(' ')

def map_matrix(line):
    line_split = line.split('\t')
    key = line_split[0]
    values = line_split[1:]
    return (key, values)

def flat_recom(tuple):
    key = tuple[0]
    value = int(tuple[1])
    tuples=[]
    global mt
    uv = mt[key]
    for i in uv:
        j = i.split(',')
        tuples.append((j[0], value * int(j[1])))
    return tuples

def reduce_recom(value_a, value_b):
    return value_a + value_b

def recom(matrix_file_name, user_file_name, output="re.out"):
    sc = SparkContext("local[8]", "Recommendation")
    """ Reads in a sequence file FILE_NAME to be manipulated """
    matrix = sc.sequenceFile(matrix_file_name)
    user = sc.sequenceFile(user_file_name)

    """
    - flatMap takes in a function that will take one input and outputs 0 or more
      items
    - map takes in a function that will take one input and outputs a single item
    - reduceByKey takes in a function, groups the dataset by keys and aggregates
      the values of each key
    """
    user_tuples = user.flatMap(flat_user) \
                 .map(map_user) \
                 .sortByKey(keyfunc=lambda k: int(k))

    keys = user_tuples.keys().collect()

    matrix_tuples = matrix.flatMap(flat_matrix) \
                          .map(map_matrix) \
                          .filter(lambda x: x[0] in keys)
    global mt 
    mt = matrix_tuples.collectAsMap()

    recm = user_tuples.flatMap(flat_recom) \
                      .reduceByKey(reduce_recom) \
                      .filter(lambda x: x[0] not in keys) \
                      .sortBy(lambda (key, value): int(value))
 
    """ Takes the dataset stored in counts and writes everything out to OUTPUT """
    recm.coalesce(1).saveAsTextFile(output)

""" Do not worry about this """
if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 3:
        recom(argv[1], argv[2])
    else:
        recom(argv[1], argv[2], argv[3])
