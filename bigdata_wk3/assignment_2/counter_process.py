#! /usr/bin/env python

import sys

# Your functions may be here.


if __name__ == '__main__':

    word_count = 0
    stop_word_count = 0

    for line in sys.stdin:
        # print(line)
        line_word_count, line_stop_word_count = line.split('\t', 1)
        # print('stop word count: ', str(line_stop_word_count))
        word_count += int(line_word_count.strip())
        stop_word_count += int(line_stop_word_count.strip())

    stop_word_percentage = float(stop_word_count) / float(word_count) * 100

    print "%.3f" % (stop_word_percentage)
