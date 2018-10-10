# %%writefile mapper1.py
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
        article_id, text = line.strip().split('\t', 1)
        words = re.split('\W*\s+\W*', text.strip().lower())
    except ValueError as e:
        continue

    for word in words:
        if word not in stop_words:
            word_list = list(word)
            word_id = ''.join(sorted(word_list))

            print(word_id + '\t' + word + '\t' + str(1))
