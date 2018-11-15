import re
import math
from pyspark import SparkConf, SparkContext

sc = SparkContext(conf=SparkConf().setAppName("TfIdf").setMaster("local"))
article = 12
term = 'labor'
stop_word_path = "stop_words_en.txt"
wiki_path = "input1.txt"
# wiki_path = "/data/wiki/en_articles_part"
# stop_word_path = "/datasets/stop_words_en.txt"


def load_stop_words(path):
    stop_words = []
    input_file = open(path, "r")
    for word in input_file:
        stop_words.append(word.rstrip("\n"))
    return stop_words


def parse_article(line):
    try:
        article_id, text = line.rstrip().split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text).lower()
        words = re.split("\W*\s+\W*", text)
        return words
    except ValueError as e:
        return []


def filter_stop_words(words):
    output_words = []
    for word in words:
        if word not in stop_word_bcast.value:
            output_words.append(word)
    return output_words


def get_word_tuples(words):
    word_tuples_list = []
    for word in words:
        word_tuples_list.append((word, 1))
    return word_tuples_list


stop_word_list = load_stop_words(stop_word_path)
stop_word_bcast = sc.broadcast(stop_word_list)

wiki = sc.textFile(wiki_path).map(parse_article)
filtered_wiki = wiki.map(filter_stop_words)
filtered_wiki.cache()



#
# def get_word_probs(input_tuple):
#     word, count = input_tuple
#     prob = float(count) / float(total_count_bcast.value)
#     return word, prob
#
#
# def get_bigram_probs(input_tuple):
#     bigram, count = input_tuple
#     prob = float(count) / float(total_bigram_count_bcast.value)
#     return bigram, prob
#
#
# def calc_npmi(input_tuple):
#     bigram, bigram_prob = input_tuple
#     bigram_words = bigram.split("_")
#     word_1_prob = word_probs_bcast.value[bigram_words[0]]
#     word_2_prob = word_probs_bcast.value[bigram_words[1]]
#     pmi = math.log(bigram_prob / (word_1_prob * word_2_prob))
#     npmi = pmi / -math.log(bigram_prob)
#     return bigram, npmi


# stop_word_list = load_stop_words(stop_word_path)
# stop_word_bcast = sc.broadcast(stop_word_list)
#
# wiki = sc.textFile(wiki_path).map(parse_article)
# filtered_wiki = wiki.map(filter_stop_words)
# filtered_wiki.cache()

total_count = wiki.count()
total_count_bcast = sc.broadcast(total_count)

word_tuples = filtered_wiki.flatMap(get_word_tuples)
word_counts = word_tuples.reduceByKey(lambda x, y: x + y)
word_probabilities = word_counts.map(get_word_probs).collectAsMap()
word_probs_bcast = sc.broadcast(word_probabilities)

bigram_tuples = filtered_wiki.flatMap(get_bigrams)
bigram_counts = bigram_tuples.reduceByKey(lambda x, y: x + y)

total_bigram_count = bigram_counts.count()
total_bigram_count_bcast = sc.broadcast(total_bigram_count)

filtered_bigrams = bigram_counts.filter(lambda x: x[1] >= bigram_filter_threshold)
bigram_probs = filtered_bigrams.map(get_bigram_probs)
bigram_npmis = bigram_probs.map(calc_npmi)
bigram_npmis_sorted = bigram_npmis.sortBy(lambda x: -x[1])

for bigram in bigram_npmis_sorted.collect()[:print_top_threshold]:
    print(bigram[0])
