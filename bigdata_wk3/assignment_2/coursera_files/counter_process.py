
#! /usr/bin/env python

import sys
import re

if __name__ == '__main__':
    
    word_count = 0
    stop_word_count = 0

    for line in sys.stdin:
        total_word_match = re.search(str(sys.argv[1]) + '=(\d+)', line)
        if total_word_match:
            word_count = int(total_word_match.group(1))
        
        stop_word_match = re.search(str(sys.argv[2]) + '=(\d+)', line)
        if stop_word_match:
            stop_word_count = int(stop_word_match.group(1))

    stop_word_percentage = float(stop_word_count) / float(word_count) * 100

    print "%.3f" % (stop_word_percentage)