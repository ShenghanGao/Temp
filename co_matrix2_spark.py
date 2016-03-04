import sys
import re

avgTs = {}

def flat_Map(document):
    return document[1].strip().split("\n")

def flat_Line(line):
    line_split = line.split()
    user = line_split[0]
    del line_split[0]
    tuples = []
    n = len(line_split)
    artistsAndRatings = map(lambda s: s.split(","), line_split)
    for i in range(n):
        for j in range(i + 1, n):
            artistAndRating1 = artistsAndRatings[i]
            artistAndRating2 = artistsAndRatings[j]

            artist1 = int(artistAndRating1[0])
            artist2 = int(artistAndRating1[1])
            if artist1 > artist2:
                tmp = artistAndRating1
                artistAndRating1 = artistAndRating2
                artistAndRating2 = tmp

            key = (int(artistAndRating1[0]), int(artistAndRating2[0]))
            value = ((int(artistAndRating1[1]), int(artistAndRating2[1])) , 1)
            tuples.append((key, value))
    return tuples

def reduce_Sum(value1, value2):
    ((rating1_1, rating2_1), cnt_1) = value1
    ((rating1_2, rating2_2), cnt_2) = value2
    return ((rating1_1 + rating1_2, rating2_1 + rating2_2), cnt_1 + cnt_2)

def map_Average(tuple):
    key = tuple[0]
    ((rating1Sum, rating2Sum), cnt) = tuple[1]
    return (key, (rating1Sum / cnt, rating2Sum / cnt))

def map_MiddleMul(tuple):
    key = tuple[0]
    (ratingAvg1, ratingAvg2) = avgTs[key]

    ((rating1, rating2), cnt) = tuple[1]
    mul1 = (rating1 - ratingAvg1) * (rating2 - ratingAvg2)
    mul2 = (rating1 - ratingAvg1) ** 2
    mul3 = (rating2 - ratingAvg2) ** 2
    return (key, (mul1, mul2, mul3))

def reduce_MiddleMulSum(value1, value2):
    (mul1_1, mul2_1, mul3_1) = value1
    (mul1_2, mul2_2, mul3_3) = value1
    return (mul1_1 + mul1_2, mul2_1 + mul2_2, mul3_1 + mul3_2)

def map_sim((key, (v1, v2, v3))):
    if v2 == 0:
        v2 = 1
    if v3 == 0:
        v3 = 1
    return (key, v1 / (v2 * v3))

def co_matrix(file_name, output="co_matrix.out"):
    sc = SparkContext("local[8]", "UserArtistMatrix")
    file = sc.textFile(file_name)

    ts = file.flatMap(flat_Map)\
        .flatMap(flat_Line)
 
    ts = file..flatMap(flat_Line)
    reducedTs = ts.reduceByKey(reduce_Sum)
    global avgTs
    avgTs = reducedTs.map(map_Average).collectAsMap()
    middleMul = ts.map(map_MiddleMul)
    reducedMulSum = middleMul.reduceByKey(reduce_MiddleMulSum)
    sim = reducedMulSum.map(map_sim)

    print sim.collect()

""" Do not worry about this """
if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        co_matrix(argv[1])
    else:
        co_matrix(argv[1], argv[2])
