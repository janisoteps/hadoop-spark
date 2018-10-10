import sys

reload(sys)
sys.setdefaultencoding('utf-8')  # required to convert to unicode

current_key = None
word_sum = 0

for line in sys.stdin:
    try:
        key, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    if current_key != key:
        if current_key:
            print "%d\t%s" % (word_sum, current_key)
        word_sum = 0
        current_key = key
    word_sum += count

if current_key:
    print "%d\t%s" % (word_sum, current_key)
