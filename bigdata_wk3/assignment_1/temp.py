%%writefile mapper1.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
        text = re.sub(r'[^\w\s]', '', text)
    except ValueError as e:
        continue
    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    for word in words:
        print >> sys.stderr, "reporter:counter:Wiki stats,Total words,%d" % 1
        print "%s\t%d" % (word.lower(), 1)







%%writefile reducer1.py

import sys

current_key = None
word_sum = 0

for line in sys.stdin:
    try:
        key, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    if current_key != key:
        if current_key:
            print "%s\t%d" % (current_key, word_sum)
        word_sum = 0
        current_key = key
    word_sum += count

if current_key:
    print "%s\t%d" % (current_key, word_sum)



%%writefile combiner1.py

import sys

current_key = None
word_sum = 0

for line in sys.stdin:
    try:
        key, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    if current_key != key:
        if current_key:
            print "%s\t%d" % (current_key, word_sum)
        word_sum = 0
        current_key = key
    word_sum += count

if current_key:
    print "%s\t%d" % (current_key, word_sum)



%%writefile mapper2.py

import sys
import re

current_key = None
word_sum = 0

for line in sys.stdin:
    try:
        key, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    if current_key != key:
        if current_key:
            print "%d\t%s" % (word_sum, current_key)
        word_sum = 0
        current_key = key
    word_sum += count

if current_key:
    print "%d\t%s" % (word_sum, current_key)



%%writefile reducer2.py

import sys

current_word = None
word_sum = 0

for line in sys.stdin:
    try:
        count, word = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    if current_word != word:
        if current_word:
            print "%s\t%d" % (current_word, word_sum)
        word_sum = 0
        current_word = word
    word_sum += count

if current_word:
    print "%s\t%d" % (current_word, word_sum)




%%bash

OUT_DIR="assignment1_"$(date +"%s%6N")
OUT_DIR2="assignment1_b_"$(date +"%s%6N")

# Code for your first job
yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files mapper1.py,reducer1.py,combiner1.py \
    -mapper "python mapper1.py" \
    -combiner "python combiner1.py" \
    -reducer "python reducer1.py" \
    -numReduceTasks 2 \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null

# Code for your second job
yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options="-k1,2nr" \
    -files mapper2.py,reducer2.py \
    -mapper "python mapper2.py" \
    -reducer "python reducer2.py" \
    -numReduceTasks 1 \
    -input ${OUT_DIR} \
    -output ${OUT_DIR2} > /dev/null

# Code for obtaining the results
hdfs dfs -cat ${OUT_DIR2}/part-00000 | sed -n "7p;8q"
hdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null
hdfs dfs -rm -r -skipTrash ${OUT_DIR2}* > /dev/null