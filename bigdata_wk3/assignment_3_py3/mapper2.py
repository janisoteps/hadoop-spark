# %%writefile mapper2.py
#!/usr/bin/env python3

import sys

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
            print(str(word_sum) + '\t' + str(capitalized_sum) + '\t' + str(current_key))
            # print('reporter:counter:Personal Counters,Mappertwo out,' + str(1), file=sys.stderr)

        word_sum = count
        capitalized_sum = capitalized_count
        current_key = key
    else:
        word_sum += count
        capitalized_sum += capitalized_count

if current_key:
    print(str(word_sum) + '\t' + str(capitalized_sum) + '\t' + str(current_key))
    # print('reporter:counter:Personal Counters,Mappertwo out,' + str(1), file=sys.stderr)
