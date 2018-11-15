
# coding: utf-8

# In[1]:


get_ipython().run_cell_magic('writefile', 'mapper_1.py', '#!/usr/bin/env python3\n\nimport sys\nimport re\n\nsw_path = \'stop_words_en.txt\'\nstop_words = set()\nstop_word_file = open(sw_path, \'r\')\nfor stop_word in stop_word_file:\n    stop_words.add(stop_word.strip())\n\nfor line in sys.stdin:\n    try:\n        article_id, text = line.rstrip().split(\'\\t\', 1)\n        text = re.sub("^\\W+|\\W+$", "", text).lower()\n        words = re.split("\\W*\\s+\\W*", text)\n    except ValueError as e:\n        continue\n\n    filtered_words = [x for x in words if x not in stop_words]\n    word_count = len(filtered_words)\n    for word in filtered_words:\n            print(word + \'_\' + str(article_id) + \'\\t\' + str(word_count) + \'\\t\' + str(1))')


# In[2]:


get_ipython().run_cell_magic('writefile', 'reducer_1.py', "#!/usr/bin/env python3\n\nimport sys\n\ncurrent_word_current_doc_id = None\ncurrent_word_count = 0\ncurrent_doc_len = None\n\n\nfor line in sys.stdin:\n    try:\n        input_word_input_doc_id, input_doc_len, input_word_count = line.strip().split('\\t', 2)\n        input_word_count = int(input_word_count)\n    except ValueError as e:\n        continue\n\n    if current_word_current_doc_id == input_word_input_doc_id:\n        current_word_count += input_word_count\n        current_doc_len = input_doc_len\n\n    else:\n        if current_word_current_doc_id is None:\n            current_word_current_doc_id = input_word_input_doc_id\n            current_word_count = input_word_count\n            current_doc_len = input_doc_len\n\n        else:\n            current_word, current_doc_id = current_word_current_doc_id.split('_', 1)\n            print(current_word + '\\t' + current_doc_id + '\\t' + current_doc_len + '\\t' + str(current_word_count) + '\\t' + str(1))\n\n            current_word_current_doc_id = input_word_input_doc_id\n            current_word_count = input_word_count\n            current_doc_len = input_doc_len\n\nif current_word_current_doc_id:\n    current_word, current_doc_id = current_word_current_doc_id.split('_', 1)\n    print(current_word + '\\t' + current_doc_id + '\\t' + current_doc_len + '\\t' + str(current_word_count) + '\\t' + str(1))")


# In[3]:


get_ipython().run_cell_magic('writefile', 'mapper_2.py', '#!/usr/bin/env python3\n\nimport sys\n\nfor line in sys.stdin:\n    try:\n        print(line)\n    except ValueError as e:\n        continue')


# In[4]:


get_ipython().run_cell_magic('writefile', 'reducer_2.py', "#!/usr/bin/env python3\n\nimport sys\n\ncurrent_word = None\ncurrent_doc_ids = []\ncurrent_doc_lens = []\ncurrent_word_counts = []\ncurrent_docs_with_word = 0\n\n\nfor line in sys.stdin:\n    try:\n        input_word, input_doc_id, input_doc_len, input_word_count, input_docs_with_word = line.strip().split('\\t', 4)\n        input_docs_with_word = int(input_docs_with_word)\n    except ValueError as e:\n        continue\n\n    if current_word == input_word:\n        current_doc_ids.append(input_doc_id)\n        current_doc_lens.append(input_doc_len)\n        current_word_counts.append(input_word_count)\n        current_docs_with_word += input_docs_with_word\n\n    else:\n        if current_word is None:\n            current_word = input_word\n            current_doc_ids = [input_doc_id]\n            current_doc_lens = [input_doc_len]\n            current_word_counts = [input_word_count]\n            current_docs_with_word = input_docs_with_word\n\n        else:\n            print(current_word + '\\t' + ','.join(current_doc_ids) + '\\t' + ','.join(current_doc_lens) + '\\t'\n                  + ','.join(current_word_counts) + '\\t' + str(current_docs_with_word))\n\n            current_word = input_word\n            current_doc_ids = [input_doc_id]\n            current_doc_lens = [input_doc_len]\n            current_word_counts = [input_word_count]\n            current_docs_with_word = input_docs_with_word\n\nif current_word:\n    print(current_word + '\\t' + ','.join(current_doc_ids) + '\\t' + ','.join(current_doc_lens) + '\\t'\n          + ','.join(current_word_counts) + '\\t' + str(current_docs_with_word))")


# In[5]:


get_ipython().run_cell_magic('writefile', 'find_labor_12.py', "#!/usr/bin/env python3\n\nimport sys\nimport math\n\nif __name__ == '__main__':\n\n    check_word = 'labor'\n    article_id = '12'\n\n    for line in sys.stdin:\n        input_word, input_doc_ids, input_doc_lens, input_word_counts, input_docs_with_word = line.strip().split('\\t', 4)\n        input_doc_ids_arr = input_doc_ids.split(',')\n        input_doc_lens_arr = input_doc_lens.split(',')\n        input_word_counts_arr = input_word_counts.split(',')\n\n        if input_word == check_word:\n            doc_id_index = input_doc_ids_arr.index(article_id)\n            doc_len = int(input_doc_lens_arr[doc_id_index])\n            word_count = int(input_word_counts_arr[doc_id_index])\n            docs_with_word = int(input_docs_with_word)\n\n            tf = word_count / doc_len\n            idf = 1 / math.log(1 + docs_with_word)\n            tf_idf = tf * idf\n\n            print(tf_idf)")


# In[6]:


get_ipython().run_cell_magic('bash', '', '\nOUT_DIR_1="week_6_tf_idf_a_"$(date +"%s%6N")\nOUT_DIR_2="week_6_tf_idf_b_"$(date +"%s%6N")\nNUM_REDUCERS_1=8\nNUM_REDUCERS_2=1\n\n# Code for first job\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -files mapper_1.py,reducer_1.py,/datasets/stop_words_en.txt \\\n    -mapper \'python3 mapper_1.py\' \\\n    -reducer \'python3 reducer_1.py\' \\\n    -numReduceTasks ${NUM_REDUCERS_1} \\\n    -input /data/wiki/en_articles_part \\\n    -output ${OUT_DIR_1} > /dev/null\n\n# Code for second job\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -files mapper_2.py,reducer_2.py,find_labor_12.py \\\n    -mapper \'python3 mapper_2.py\' \\\n    -reducer \'python3 reducer_2.py\' \\\n    -numReduceTasks ${NUM_REDUCERS_2} \\\n    -input ${OUT_DIR_1} \\\n    -output ${OUT_DIR_2} > /dev/null\n\n# Code for obtaining the results\nhdfs dfs -cat ${OUT_DIR_2}/part-00000 | python3 ./find_labor_12.py\n\nhdfs dfs -rm -r -skipTrash ${OUT_DIR_1}* > /dev/null\nhdfs dfs -rm -r -skipTrash ${OUT_DIR_2}* > /dev/null')

