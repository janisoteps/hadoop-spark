# %%writefile reducer2.py
# #!/usr/bin/env python3

import sys

total_count = 0
total_caps = 0
current_word = None

for line in sys.stdin:

    count, caps_count, key = line.strip().split('\t', 2)
    count = float(count)
    caps_count = float(caps_count)

    if key == current_word:
        total_caps += caps_count
        total_count += count
    else:
        if current_word is None:
            current_word = key
            total_caps = caps_count
            total_count = count

        else:
            # print(f'{current_word} total caps: {total_caps}, total count: {total_count}')
            caps_ratio = float(total_caps) / float(total_count)
            # print(f'{current_word} caps ratio:{caps_ratio}')
            if caps_ratio > 0.995:
                print(str(current_word) + '\t' + str(int(total_caps)))

            total_count = count
            total_caps = caps_count
            current_word = key

caps_ratio = float(total_caps) / float(total_count)
if caps_ratio > 0.995:
    print(str(current_word) + '\t' + str(int(total_caps)))