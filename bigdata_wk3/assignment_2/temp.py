% % writefile
mapper.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')  # required to convert to unicode

path = 'stop_words_en.txt'

stop_words = set()
stop_word_file = open(path, 'r')
for stop_word in stop_word_file:
    stop_words.add(stop_word.strip())

# print(stop_words)
for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
    except ValueError as e:
        continue

    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)

    stop_word_count = 0
    word_count = 0

    for word in words:
        word_count += 1
        if word.lower() in stop_words:
            stop_word_count += 1

    print "%d\t%d" % (word_count, stop_word_count)



















%%writefile reducer.py

import sys

word_sum = 0
stop_word_sum = 0

for line in sys.stdin:
    try:
        word_count, stop_word_count = line.strip().split('\t', 1)
        word_count = int(word_count)
        stop_word_count = int(stop_word_count)
    except ValueError as e:
        continue
    word_sum += word_count
    stop_word_sum += stop_word_count

print "%d\t%d" % (word_sum, stop_word_sum)
print >> sys.stderr, "reporter:counter:Personal Counters,Total words,%d" % word_sum
print >> sys.stderr, "reporter:counter:Personal Counters,Stop words,%d" % stop_word_sum

% % writefile
counter_process.py

# ! /usr/bin/env python

import sys
import re

if __name__ == '__main__':

    word_count = 0
    stop_word_count = 0

    for line in sys.stdin:
        total_word_match = re.search(str(sys.argv[1]) + '=(\d+)', line)
        if total_word_match:
            word_count = int(total_word_match.group(1))

        stop_word_match = re.search(str(sys.argv[2]) + '=(\d+)', line)
        if stop_word_match:
            stop_word_count = int(stop_word_match.group(1))

    stop_word_percentage = float(stop_word_count) / float(word_count) * 100

    print "%.3f" % (stop_word_percentage)






















%%bash

OUT_DIR="coursera_mr_task2"$(date +"%s%6N")
NUM_REDUCERS=8
LOGS="stderr_logs.txt"

# Stub code for your job

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files mapper.py,reducer.py,counter_process.py,/datasets/stop_words_en.txt,${LOGS} \
    -mapper "/usr/bin/python2 mapper.py" \
    -reducer "/usr/bin/python2 reducer.py" \
    -numReduceTasks ${NUM_REDUCERS} \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null 2> ${LOGS}
# ... \
#    -output ${OUT_DIR} > /dev/null 2> $LOGS
cat ${LOGS} | /usr/bin/python2 ./counter_process.py "Total words" "Stop words"
cat ${LOGS} >&2

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null
