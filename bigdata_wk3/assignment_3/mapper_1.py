# %%writefile mapper1.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')  # required to convert to unicode

for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
        text = re.sub(r'[^\w\s]', '', text)
    except ValueError as e:
        continue

    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    for word in words:

        capitalised = 0
        if word[0] != word[0].lower():
            capitalised = 1

        first_digit = word[0].isdigit()

        if not first_digit:
            print "%s\t%d\t%d" % (word.lower(), 1, capitalised)
