# %%writefile reducer_2.py
# #!/usr/bin/env python3

import sys

current_word = None
current_doc_ids = []
current_doc_lens = []
current_word_counts = []
current_docs_with_word = 0


for line in sys.stdin:
    try:
        input_word, input_doc_id, input_doc_len, input_word_count, input_docs_with_word = line.strip().split('\t', 4)
        input_docs_with_word = int(input_docs_with_word)
    except ValueError as e:
        continue

    if current_word == input_word:
        current_doc_ids.append(input_doc_id)
        current_doc_lens.append(input_doc_len)
        current_word_counts.append(input_word_count)
        current_docs_with_word += input_docs_with_word

    else:
        if current_word is None:
            current_word = input_word
            current_doc_ids = [input_doc_id]
            current_doc_lens = [input_doc_len]
            current_word_counts = [input_word_count]
            current_docs_with_word = input_docs_with_word

        else:
            print(current_word + '\t' + ','.join(current_doc_ids) + '\t' + ','.join(current_doc_lens) + '\t'
                  + ','.join(current_word_counts) + '\t' + str(current_docs_with_word))

            current_word = input_word
            current_doc_ids = [input_doc_id]
            current_doc_lens = [input_doc_len]
            current_word_counts = [input_word_count]
            current_docs_with_word = input_docs_with_word

if current_word:
    print(current_word + '\t' + ','.join(current_doc_ids) + '\t' + ','.join(current_doc_lens) + '\t'
          + ','.join(current_word_counts) + '\t' + str(current_docs_with_word))
