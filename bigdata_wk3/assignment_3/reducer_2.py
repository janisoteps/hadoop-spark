import sys

current_key = None
word_sum = 0
capitalized_sum = 0

for line in sys.stdin:
    try:
        count, capitalized_count, key = line.strip().split('\t', 2)
        count = int(count)
        capitalized_count = int(capitalized_count)
        # print(key, str(count), str(capitalized_count))

    except ValueError as e:
        continue

    if current_key != key and capitalized_count > 0:
        capitalized_ratio = float(capitalized_sum) / float(word_sum)
        # print(current_key, str(capitalized_ratio))

        if current_key and capitalized_ratio > 0.995:
            print "%s\t%d" % (current_key, word_sum)
        word_sum = 0
        capitalized_sum = 0
        current_key = key

    word_sum += count
    capitalized_sum += capitalized_count

capitalized_ratio = 0
if capitalized_sum > 0:
    capitalized_ratio = float(capitalized_sum) / float(word_sum)

# print(current_key, str(capitalized_ratio))
if current_key and capitalized_ratio > 0.995:
    print "%s\t%d" % (current_key, word_sum)
