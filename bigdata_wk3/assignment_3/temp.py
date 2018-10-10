%%writefile mapper1.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')  # required to convert to unicode

for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
        text = re.sub(r'[^\w\s]', '', text)
    except ValueError as e:
        continue

    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    for word in words:

        capitalised = 0
        if word[0] != word[0].lower():
            capitalised = 1

        first_digit = word[0].isdigit()

        if not first_digit:
            print "%s\t%d\t%d" % (word.lower(), 1, capitalised)











%%writefile combiner1.py

current_key = None
word_sum = 0
capitalized_sum = 0

for line in sys.stdin:
    try:
        key, count, capitalized_count = line.strip().split('\t', 2)
        count = int(count)
        capitalized_count = int(capitalized_count)
    # print('capitalized count: ', str(capitalized_count))
    except ValueError as e:
        continue

    if current_key != key:
        if current_key:
            print "%s\t%d\t%d" % (current_key, word_sum, capitalized_sum)
        word_sum = 0
        capitalized_sum = 0

        current_key = key

    word_sum += count
    capitalized_sum += capitalized_count

if current_key:
    print "%s\t%d\t%d" % (current_key, word_sum, capitalized_sum)












%%writefile reducer1.py

current_key = None
word_sum = 0
capitalized_sum = 0

for line in sys.stdin:
    try:
        key, count, capitalized_count = line.strip().split('\t', 2)
        count = int(count)
        capitalized_count = int(capitalized_count)

    except ValueError as e:
        continue

    if current_key != key:
        if current_key:
            print "%s\t%d\t%d" % (current_key, word_sum, capitalized_sum)
        word_sum = 0
        capitalized_sum = 0
        current_key = key

    word_sum += count
    capitalized_sum += capitalized_count

if current_key:
    print "%s\t%d\t%d" % (current_key, word_sum, capitalized_sum)















%%writefile mapper2.py

current_key = None
word_sum = 0
capitalized_sum = 0

for line in sys.stdin:
    try:
        key, count, capitalized_count = line.strip().split('\t', 2)
        count = int(count)
        capitalized_count = int(capitalized_count)

    except ValueError as e:
        continue

    if current_key != key:
        if current_key:
            print "%d\t%d\t%s" % (word_sum, capitalized_sum, current_key)
        word_sum = 0
        capitalized_sum = 0
        current_key = key

    word_sum += count
    capitalized_sum += capitalized_count

if current_key:
    print "%d\t%d\t%s" % (word_sum, capitalized_sum, current_key)

















%%writefile reducer2.py

current_key = None
word_sum = 0
capitalized_sum = 0

for line in sys.stdin:
    try:
        count, capitalized_count, key = line.strip().split('\t', 2)
        count = int(count)
        capitalized_count = int(capitalized_count)
        # print(key, str(count), str(capitalized_count))

    except ValueError as e:
        continue

    if current_key != key and capitalized_count > 0:
        capitalized_ratio = float(capitalized_sum) / float(word_sum)
        # print(current_key, str(capitalized_ratio))

        if current_key and capitalized_ratio > 0.995:
            print "%s\t%d" % (current_key, word_sum)
        word_sum = 0
        capitalized_sum = 0
        current_key = key

    word_sum += count
    capitalized_sum += capitalized_count

capitalized_ratio = 0
if capitalized_sum > 0:
    capitalized_ratio = float(capitalized_sum) / float(word_sum)

# print(current_key, str(capitalized_ratio))
if current_key and capitalized_ratio > 0.995:
    print "%s\t%d" % (current_key, word_sum)






















%%bash

OUT_DIR="assignment3_"$(date +"%s%6N")
OUT_DIR2="assignment3_b_"$(date +"%s%6N")

# Code for your first job
yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files mapper1.py,reducer1.py,combiner1.py \
    -mapper '/usr/bin/python2 mapper1.py' \
    -combiner '/usr/bin/python2 combiner1.py' \
    -reducer '/usr/bin/python2 reducer1.py' \
    -numReduceTasks 2 \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null

# Code for your second job
yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options="-k1,3nr" \
    -files mapper2.py,reducer2.py \
    -mapper '/usr/bin/python2 mapper2.py' \
    -reducer '/usr/bin/python2 reducer2.py' \
    -numReduceTasks 1 \
    -input ${OUT_DIR} \
    -output ${OUT_DIR2} > /dev/null

# Code for obtaining the results
hdfs dfs -cat ${OUT_DIR2}/part-00000 | sed -n "5p;8q"
hdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null
hdfs dfs -rm -r -skipTrash ${OUT_DIR2}* > /dev/null