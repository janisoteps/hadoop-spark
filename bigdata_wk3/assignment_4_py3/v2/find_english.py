# %%writefile find_english.py
#!/usr/bin/env python3

import sys

if __name__ == '__main__':

    check_word = 'english'

    for line in sys.stdin:
        current_count, current_id, current_words_count, current_words = line.strip().split('\t', 3)

        ordered_check_word = ''.join(sorted(list(check_word)))

        if current_id == ordered_check_word:
            print(current_count + '\t' + current_id + '\t' + current_words_count + '\t' + current_words)
