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
            print "%d\t%d\t%s" % (word_sum, capitalized_sum, current_key)
        word_sum = 0
        capitalized_sum = 0
        current_key = key

    word_sum += count
    capitalized_sum += capitalized_count

if current_key:
    print "%d\t%d\t%s" % (word_sum, capitalized_sum, current_key)