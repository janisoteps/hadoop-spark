import re
from pyspark import SparkConf, SparkContext

sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))


def parse_article(line):
    try:
        article_id, text = unicode(line.rstrip()).split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE).lower()
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        return words
    except ValueError as e:
        return []


def find_pairs(words):
    for word in words:
        # print("find pairs word: " + str(word))
        if word == "narodnaya":
            pair = word + "_" + words[words.index(word) + 1]

            return pair


# wiki = sc.textFile("/data/wiki/en_articles_part/articles-part", 16).map(parse_article)
wiki = sc.textFile("input1.txt").map(parse_article)
pairs = wiki.map(find_pairs)

pairs = pairs.collect()

pair_dict = {}
pair_list = []
for pair in pairs:
    if pair is not None:
        if pair in pair_list:
            pair_dict[pair] += 1
        else:
            pair_list.append(pair)
            pair_dict[pair] = 1

pair_list = sorted(pair_list)
for pair in pair_list:
    # print(pair)
    print(pair + "\t" + str(pair_dict[pair]))
