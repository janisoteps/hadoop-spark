import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')  # required to convert to unicode

path = 'stop_words_en.txt'

stop_words = set()
stop_word_file = open(path, 'r')
for stop_word in stop_word_file:
    stop_words.add(stop_word.strip())

# print(stop_words)
for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
    except ValueError as e:
        continue

    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)

    stop_word_count = 0
    word_count = 0

    for word in words:
        word_count += 1
        if word.lower() in stop_words:
            stop_word_count += 1

    print "%d\t%d" % (word_count, stop_word_count)