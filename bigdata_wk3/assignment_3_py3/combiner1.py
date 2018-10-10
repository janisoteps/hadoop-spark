# %%writefile combiner1.py
#!/usr/bin/env python3


import sys

current_key = None
word_sum = 0
capitalized_sum = 0

for line in sys.stdin:
    # print('reporter:counter:Personal Counters,Combiner in,' + str(1), file=sys.stderr)
    try:
        key, count, capitalized_count = line.strip().split('\t', 2)
        count = int(count)
        capitalized_count = int(capitalized_count)
    # print('capitalized count: ', str(capitalized_count))
    except ValueError as e:
        continue

    if current_key != key:
        if current_key:
            print(str(current_key) + '\t' + str(word_sum) + '\t' + str(capitalized_sum))
            # print('reporter:counter:Personal Counters,Combiner out,' + str(1), file=sys.stderr)

        word_sum = count
        capitalized_sum = capitalized_count
        current_key = key
    else:
        word_sum += count
        capitalized_sum += capitalized_count

if current_key:
    print(str(current_key) + '\t' + str(word_sum) + '\t' + str(capitalized_sum))
    # print('reporter:counter:Personal Counters,Combiner out,' + str(1), file=sys.stderr)

