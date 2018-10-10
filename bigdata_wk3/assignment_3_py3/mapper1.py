# %%writefile mapper1.py
# #!/usr/bin/env python3

import sys
import re

# print('reporter:counter:Personal Counters,Mapper started,' + str(1), file=sys.stderr)

for line in sys.stdin:
    # print('reporter:counter:Personal Counters,Lines in,' + str(1), file=sys.stderr)

    article_id, text = line.strip().split('\t', 1)
#     text = re.sub('[^\w\s]', ' ', text)

    try:
        words = re.split('\W*\s+\W*', text)
    except:
        continue

    for word in words:
        # print('reporter:counter:Personal Counters,Words in,' + str(1), file=sys.stderr)
        capitalised = None
        try:
            if not word[0].islower() and word[1:].islower():
                capitalised = 1
            else:
                capitalised = 0
        except:
            continue

        if not word[0].isdigit() and capitalised is not None:
            # print('reporter:counter:Personal Counters,Total words,' + str(1), file=sys.stderr)
            # print('reporter:counter:Personal Counters,Capitalized words,' + str(capitalised), file=sys.stderr)
            print(word.lower() + '\t' + str(1) + '\t' + str(capitalised))
