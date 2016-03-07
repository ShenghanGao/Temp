import sys
import re
import math

from pyspark import SparkContext

def mapLine(line):
    m = {}
    sp = line.split(" ")
    artist = sp[0]
    userAndRatings = sp[1].split("\t")
    for split in userAndRatings:
        [user, rating] = split.split(",")
        m[user] = rating
    return (artist, m)

def mapToPearson(((artist1, userAndRatings1), (artist2, userAndRatings2))):
    intersectionKeySet = set(userAndRatings1.keys()) & set(userAndRatings2.keys())
    length = len(intersectionKeySet)
    if length == 0:
        return ((artist1, artist2), 0)

    avg1 = 0
    avg2 = 0
    for key in intersectionKeySet:
        avg1 += int(userAndRatings1[key])
        avg2 += int(userAndRatings2[key])
    avg1 = float(avg1) / length
    avg2 = float(avg2) / length

    numerator = 0
    s1 = 0
    s2 = 0
    for key in intersectionKeySet:
        numerator += (int(userAndRatings1[key]) - avg1) * (int(userAndRatings2[key]) - avg2)
        s1 += (int(userAndRatings1[key]) - avg1) ** 2
        s2 += (int(userAndRatings2[key]) - avg2) ** 2

    if s1 == 0 or s2 == 0:
        return ((artist1, artist2), 0)

    s1 = float(s1)
    s2 = float(s2)
    denominator = math.sqrt(s1 * s2)

    return ((artist1, artist2), numerator / denominator)

def co_matrix(file_name, output="co_matrix.out"):
    sc = SparkContext("local[8]", "UserArtistMatrix")
    file = sc.textFile(file_name)

    artistCom = file.map(mapLine)
    print "ARTISTCOM_COUNT", artistCom.count()
    cartArtistCom = artistCom.cartesian(artistCom)
    print "CART_ARTIST_COUNT", cartArtistCom.count()
    filteredArtistCom = cartArtistCom.filter(lambda x: x[0][0] <= x[1][0])
    co_pearson = filteredArtistCom.map(mapToPearson)
    # print "FILTERED_ARTIST_COUNT", filteredArtistCom.count()
    # print filteredArtistCom.collect()
    print "CO_PEARSON_COUNT", co_pearson.count()
    print "CO_PEARSON\n", co_pearson.collect()
    print "No error!!!"

""" Do not worry about this """
if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        co_matrix(argv[1])
    else:
        co_matrix(argv[1], argv[2])

