
import sys

word_sum = 0
stop_word_sum = 0

for line in sys.stdin:
    try:
        word_count, stop_word_count = line.strip().split('\t', 1)
        word_count = int(word_count)
        stop_word_count = int(stop_word_count)
    except ValueError as e:
        continue
    word_sum += word_count
    stop_word_sum += stop_word_count

print "%d\t%d" % (word_sum, stop_word_sum)
print >> sys.stderr, "reporter:counter:Personal Counters,Total words,%d" % word_sum
print >> sys.stderr, "reporter:counter:Personal Counters,Stop words,%d" % stop_word_sum