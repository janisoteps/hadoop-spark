# %%writefile mapper_1.py
# #!/usr/bin/env python3

import sys
import re

sw_path = 'stop_words_en.txt'
stop_words = set()
stop_word_file = open(sw_path, 'r')
for stop_word in stop_word_file:
    stop_words.add(stop_word.strip())

for line in sys.stdin:
    try:
        article_id, text = line.rstrip().split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text).lower()
        words = re.split("\W*\s+\W*", text)
    except ValueError as e:
        continue

    filtered_words = [x for x in words if x not in stop_words]
    word_count = len(filtered_words)
    for word in filtered_words:
            print(word + '_' + str(article_id) + '\t' + str(word_count) + '\t' + str(1))
