# %%writefile combiner1.py
#!/usr/bin/env python3

import sys

current_id = None
current_count = 0
current_words = []

for line in sys.stdin:
    try:
        input_id, input_word, input_count = line.strip().split('\t', 2)
        input_count = int(input_count)
    except ValueError as e:
        continue

    if current_id == input_id:
        current_count += input_count

        if input_word not in current_words:
            current_words.append(input_word)

    else:
        if current_id is None:
            current_id = input_id
            current_count = input_count
            current_words = [input_word]

        else:
            print(current_id + '\t' + ','.join(current_words) + '\t' + str(current_count))

            current_id = input_id
            current_count = input_count
            current_words = [input_word]

if current_id:
    print(current_id + '\t' + ','.join(current_words) + '\t' + str(current_count))
