
# coding: utf-8

# In[1]:


get_ipython().run_cell_magic('writefile', 'mapper1.py', "#!/usr/bin/env python3\n\nimport sys\nimport re\n\n# print('reporter:counter:Personal Counters,Mapper started,' + str(1), file=sys.stderr)\n\nfor line in sys.stdin:\n    # print('reporter:counter:Personal Counters,Lines in,' + str(1), file=sys.stderr)\n\n    article_id, text = line.strip().split('\\t', 1)\n#     text = re.sub('[^\\w\\s]', ' ', text)\n\n    try:\n        words = re.split('\\W*\\s+\\W*', text)\n    except:\n        continue\n\n    for word in words:\n        # print('reporter:counter:Personal Counters,Words in,' + str(1), file=sys.stderr)\n        capitalised = None\n        try:\n            if not word[0].islower() and word[1:].islower():\n                capitalised = 1\n            else:\n                capitalised = 0\n        except:\n            continue\n\n        if not word[0].isdigit() and capitalised is not None:\n            # print('reporter:counter:Personal Counters,Total words,' + str(1), file=sys.stderr)\n            # print('reporter:counter:Personal Counters,Capitalized words,' + str(capitalised), file=sys.stderr)\n            print(word.lower() + '\\t' + str(1) + '\\t' + str(capitalised))")


# In[2]:


get_ipython().run_cell_magic('writefile', 'combiner1.py', "#!/usr/bin/env python3\n\nimport sys\n\ncurrent_key = None\nword_sum = 0\ncapitalized_sum = 0\n\nfor line in sys.stdin:\n    # print('reporter:counter:Personal Counters,Combiner in,' + str(1), file=sys.stderr)\n    try:\n        key, count, capitalized_count = line.strip().split('\\t', 2)\n        count = int(count)\n        capitalized_count = int(capitalized_count)\n    # print('capitalized count: ', str(capitalized_count))\n    except ValueError as e:\n        continue\n\n    if current_key != key:\n        if current_key:\n            print(str(current_key) + '\\t' + str(word_sum) + '\\t' + str(capitalized_sum))\n            # print('reporter:counter:Personal Counters,Combiner out,' + str(1), file=sys.stderr)\n\n        word_sum = count\n        capitalized_sum = capitalized_count\n        current_key = key\n    else:\n        word_sum += count\n        capitalized_sum += capitalized_count\n\nif current_key:\n    print(str(current_key) + '\\t' + str(word_sum) + '\\t' + str(capitalized_sum))\n    # print('reporter:counter:Personal Counters,Combiner out,' + str(1), file=sys.stderr)")


# In[3]:


get_ipython().run_cell_magic('writefile', 'reducer1.py', "#!/usr/bin/env python3\n\nimport sys\n\ncurrent_key = None\nword_sum = 0\ncapitalized_sum = 0\n\nfor line in sys.stdin:\n    try:\n        key, count, capitalized_count = line.strip().split('\\t', 2)\n        count = int(count)\n        capitalized_count = int(capitalized_count)\n\n    except ValueError as e:\n        continue\n\n    if current_key != key:\n        if current_key:\n            print(str(current_key) + '\\t' + str(word_sum) + '\\t' + str(capitalized_sum))\n            # print('reporter:counter:Personal Counters,Reducer out,' + str(1), file=sys.stderr)\n        \n        word_sum = count\n        capitalized_sum = capitalized_count\n        current_key = key\n    else:\n        word_sum += count\n        capitalized_sum += capitalized_count\n\nif current_key:\n    print(str(current_key) + '\\t' + str(word_sum) + '\\t' + str(capitalized_sum))\n    # print('reporter:counter:Personal Counters,Reducer out,' + str(1), file=sys.stderr)\n")


# In[4]:


get_ipython().run_cell_magic('writefile', 'mapper2.py', "#!/usr/bin/env python3\n\nimport sys\n\ncurrent_key = None\nword_sum = 0\ncapitalized_sum = 0\n\nfor line in sys.stdin:\n    try:\n        key, count, capitalized_count = line.strip().split('\\t', 2)\n        count = int(count)\n        capitalized_count = int(capitalized_count)\n\n    except ValueError as e:\n        continue\n\n    if current_key != key:\n        if current_key:\n            print(str(word_sum) + '\\t' + str(capitalized_sum) + '\\t' + str(current_key))\n            # print('reporter:counter:Personal Counters,Mappertwo out,' + str(1), file=sys.stderr)\n        \n        word_sum = count\n        capitalized_sum = capitalized_count\n        current_key = key\n    else:\n        word_sum += count\n        capitalized_sum += capitalized_count\n\nif current_key:\n    print(str(word_sum) + '\\t' + str(capitalized_sum) + '\\t' + str(current_key))\n    # print('reporter:counter:Personal Counters,Mappertwo out,' + str(1), file=sys.stderr)")


# In[5]:


get_ipython().run_cell_magic('writefile', 'reducer2.py', "# #!/usr/bin/env python3\n\nimport sys\n\ntotal_count = 0\ntotal_caps = 0\ncurrent_word = None\n\nfor line in sys.stdin:\n\n    count, caps_count, key = line.strip().split('\\t', 2)\n    count = float(count)\n    caps_count = float(caps_count)\n\n    if key == current_word:\n        total_caps += caps_count\n        total_count += count\n    else:\n        if current_word is None:\n            current_word = key\n            total_caps = caps_count\n            total_count = count\n\n        else:\n            # print(f'{current_word} total caps: {total_caps}, total count: {total_count}')\n            caps_ratio = float(total_caps) / float(total_count)\n            # print(f'{current_word} caps ratio:{caps_ratio}')\n            if caps_ratio > 0.995:\n                print(str(current_word) + '\\t' + str(int(total_caps)))\n\n            total_count = count\n            total_caps = caps_count\n            current_word = key\n\ncaps_ratio = float(total_caps) / float(total_count)\nif caps_ratio > 0.995:\n    print(str(current_word) + '\\t' + str(int(total_caps)))")


# In[6]:


get_ipython().run_cell_magic('bash', '', '\nOUT_DIR="assignment3_"$(date +"%s%6N")\nOUT_DIR2="assignment3_b_"$(date +"%s%6N")\nLOGS="stderr_logs.txt"\nLOGS2="stderr_logs2.txt"\nNUM_REDUCERS=4\nREDUCER_OUTPUT="reducer_output.txt"\n\n# Code for your first job\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -files mapper1.py,reducer1.py,combiner1.py \\\n    -mapper \'python3 mapper1.py\' \\\n    -combiner \'python3 combiner1.py\' \\\n    -reducer \'python3 reducer1.py\' \\\n    -numReduceTasks ${NUM_REDUCERS} \\\n    -input /data/wiki/en_articles_part \\\n    -output ${OUT_DIR} > /dev/null 2> ${LOGS}\n\n# Code for your second job\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \\\n    -D mapreduce.partition.keycomparator.options="-k1,3nr" \\\n    -files mapper2.py,reducer2.py \\\n    -mapper \'python3 mapper2.py\' \\\n    -reducer \'python3 reducer2.py\' \\\n    -numReduceTasks 1 \\\n    -input ${OUT_DIR} \\\n    -output ${OUT_DIR2} > /dev/null 2> ${LOGS2}\n\n# Code for obtaining the results\nhdfs dfs -cat ${OUT_DIR2}/part-00000 | sed -n "5p;8q"\n# hdfs dfs -cat ${OUT_DIR2}/part-00000 > ${REDUCER_OUTPUT}\n\nhdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null\nhdfs dfs -rm -r -skipTrash ${OUT_DIR2}* > /dev/null')

