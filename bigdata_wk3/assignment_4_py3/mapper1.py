# %%writefile mapper1.py
# #!/usr/bin/env python3

import sys
import re
import numpy as np

chars = []  # A list of encountered characters, used as dictionary of chars to indexes
sw_path = 'stop_words_en.txt'
stop_words = set()
stop_word_file = open(sw_path, 'r')
for stop_word in stop_word_file:
    stop_words.add(stop_word.strip())


def translate(char):
    if char in chars:
        return chars.index(char)
    else:
        chars.append(char)
        return chars.index(char)


for line in sys.stdin:
    article_id, text = line.strip().split('\t', 1)
    #  text = re.sub('[^\w\s]', ' ', text)
    word_data = {}
    word_arrays = {}

    try:
        words = re.split('\W*\s+\W*', text.strip().lower())
    except:
        continue

    for word in words:
        if word in stop_words:
            word_list = list(word)
            ordered_word = sorted(word_list)
            word_array = np.array(list(map(lambda x: translate(x), ordered_word)))
            word_arrays[word] = word_array

    for word in words:
        if word in stop_words:
            word_array = word_arrays[word]
            word_list = list(word)
            ordered_word = sorted(word_list)
            word_id = ''.join(ordered_word)

            try:
                data_check = word_data[word_id]
            except:
                data_check = None

            if data_check is None:
                word_data[word_id] = {
                    'count': 1,
                    'words': [word]
                }

                for word_inner in words:
                    if word_inner in stop_words:
                        word_inner_array = word_arrays[word_inner]
                        distance = np.sum(np.subtract(word_inner_array, word_array))

                        if distance == 0:
                            word_data[word_id]['count'] += 1
                            if word_inner not in word_data[word_id]['words']:
                                word_data[word_id]['words'].append(word_inner)

    for word_id, data in word_data.items():
        print(word_id + '\t' + str(data['count']) + '\t' + ','.join(data['words']))


