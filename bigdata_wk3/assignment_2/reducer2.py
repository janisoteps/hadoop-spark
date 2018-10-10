import sys

current_word = None
word_sum = 0

for line in sys.stdin:
    try:
        count, word = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    if current_word != word:
        if current_word:
            print "%s\t%d" % (current_word, word_sum)
        word_sum = 0
        current_word = word
    word_sum += count

if current_word:
    print "%s\t%d" % (current_word, word_sum)
