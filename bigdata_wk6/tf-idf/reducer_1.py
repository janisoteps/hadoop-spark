# %%writefile reducer_1.py
# #!/usr/bin/env python3

import sys

current_word_current_doc_id = None
current_word_count = 0
current_doc_len = None


for line in sys.stdin:
    try:
        input_word_input_doc_id, input_doc_len, input_word_count = line.strip().split('\t', 2)
        input_word_count = int(input_word_count)
    except ValueError as e:
        continue

    if current_word_current_doc_id == input_word_input_doc_id:
        current_word_count += input_word_count
        current_doc_len = input_doc_len

    else:
        if current_word_current_doc_id is None:
            current_word_current_doc_id = input_word_input_doc_id
            current_word_count = input_word_count
            current_doc_len = input_doc_len

        else:
            current_word, current_doc_id = current_word_current_doc_id.split('_', 1)
            print(current_word + '\t' + current_doc_id + '\t' + current_doc_len + '\t' + str(current_word_count) + '\t' + str(1))

            current_word_current_doc_id = input_word_input_doc_id
            current_word_count = input_word_count
            current_doc_len = input_doc_len

if current_word_current_doc_id:
    current_word, current_doc_id = current_word_current_doc_id.split('_', 1)
    print(current_word + '\t' + current_doc_id + '\t' + current_doc_len + '\t' + str(current_word_count) + '\t' + str(1))
