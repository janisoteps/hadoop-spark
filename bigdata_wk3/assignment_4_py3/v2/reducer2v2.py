# %%writefile reducer2.py
# #!/usr/bin/env python3

import sys

current_id = None
current_count = 0
current_words = []

for line in sys.stdin:
    # print('reporter:counter:Personal Counters,Combiner in,' + str(1), file=sys.stderr)
    try:
        input_id, input_count, input_words = line.strip().split('\t', 2)
        input_count = int(input_count)
        input_words = input_words.split(',')

    except ValueError as e:
        continue

    if current_id == input_id:
        current_count += input_count
        current_words = list(set(current_words + input_words))  # merge word lists
    else:
        if current_id is None:
            current_id = input_id
            current_count = input_count
            current_words = input_words
        else:
            # print(current_words)
            if len(current_words) > 1:
                print(str(current_count) + '\t' + current_id + '\t' + str(len(current_words)) + '\t' + ','.join(sorted(current_words)))

            current_id = input_id
            current_count = input_count
            current_words = input_words

if current_id and len(current_words) > 1:
    print(str(current_count) + '\t' + current_id + '\t' + str(len(current_words)) + '\t' + ','.join(sorted(current_words)))
