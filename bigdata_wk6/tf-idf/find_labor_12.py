# %%writefile find_labor_12.py
# #!/usr/bin/env python3

import sys
import math

if __name__ == '__main__':

    check_word = 'labor'
    article_id = '12'

    for line in sys.stdin:
        input_word, input_doc_ids, input_doc_lens, input_word_counts, input_docs_with_word = line.strip().split('\t', 4)
        input_doc_ids_arr = input_doc_ids.split(',')
        input_doc_lens_arr = input_doc_lens.split(',')
        input_word_counts_arr = input_word_counts.split(',')

        if input_word == check_word:
            doc_id_index = input_doc_ids_arr.index(article_id)
            doc_len = int(input_doc_lens_arr[doc_id_index])
            word_count = int(input_word_counts_arr[doc_id_index])
            docs_with_word = int(input_docs_with_word)

            tf = word_count / doc_len
            idf = 1 / math.log(1 + docs_with_word)
            tf_idf = tf * idf

            print(tf_idf)
