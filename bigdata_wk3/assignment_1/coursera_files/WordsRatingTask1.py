
# coding: utf-8

# # Hadoop Streaming assignment 1: Words Rating

# The purpose of this task is to create your own WordCount program for Wikipedia dump processing and learn basic concepts of the MapReduce.
# 
# In this task you have to find the 7th word by popularity and its quantity in the reverse order (most popular first) in Wikipedia data (`/data/wiki/en_articles_part`).
# 
# There are several points for this task:
# 
# 1) As an output, you have to get the 7th word and its quantity separated by a tab character.
# 
# 2) You must use the second job to obtain a totally ordered result.
# 
# 3) Do not forget to redirect all trash and output to /dev/null.
# 
# Here you can find the draft of the task main steps. You can use other methods for solution obtaining.

# ## Step 1. Create mapper and reducer.
# 
# <b>Hint:</b>  Demo task contains almost all the necessary pieces to complete this assignment. You may use the demo to implement the first MapReduce Job.

# In[1]:


get_ipython().run_cell_magic('writefile', 'mapper1.py', '\nimport sys\nimport re\n\nreload(sys)\nsys.setdefaultencoding(\'utf-8\') # required to convert to unicode\n\nfor line in sys.stdin:\n    try:\n        article_id, text = unicode(line.strip()).split(\'\\t\', 1)\n        text = re.sub(r\'[^\\w\\s]\', \'\', text)\n    except ValueError as e:\n        continue\n    words = re.split("\\W*\\s+\\W*", text, flags=re.UNICODE)\n    for word in words:\n        print >> sys.stderr, "reporter:counter:Wiki stats,Total words,%d" % 1\n        print "%s\\t%d" % (word.lower(), 1)')


# In[2]:


get_ipython().run_cell_magic('writefile', 'reducer1.py', '\nimport sys\n\ncurrent_key = None\nword_sum = 0\n\nfor line in sys.stdin:\n    try:\n        key, count = line.strip().split(\'\\t\', 1)\n        count = int(count)\n    except ValueError as e:\n        continue\n    if current_key != key:\n        if current_key:\n            print "%s\\t%d" % (current_key, word_sum)\n        word_sum = 0\n        current_key = key\n    word_sum += count\n\nif current_key:\n    print "%s\\t%d" % (current_key, word_sum)')


# In[3]:


get_ipython().run_cell_magic('writefile', 'combiner1.py', '\nimport sys\n\ncurrent_key = None\nword_sum = 0\n\nfor line in sys.stdin:\n    try:\n        key, count = line.strip().split(\'\\t\', 1)\n        count = int(count)\n    except ValueError as e:\n        continue\n    if current_key != key:\n        if current_key:\n            print "%s\\t%d" % (current_key, word_sum)\n        word_sum = 0\n        current_key = key\n    word_sum += count\n\nif current_key:\n    print "%s\\t%d" % (current_key, word_sum)')


# ## Step 2. Create sort job.
# 
# <b>Hint:</b> You may use MapReduce comparator to solve this step. Make sure that the keys are sorted in ascending order.

# In[4]:


get_ipython().run_cell_magic('writefile', 'mapper2.py', '\nimport sys\nimport re\n\ncurrent_key = None\nword_sum = 0\n\nfor line in sys.stdin:\n    try:\n        key, count = line.strip().split(\'\\t\', 1)\n        count = int(count)\n    except ValueError as e:\n        continue\n    if current_key != key:\n        if current_key:\n            print "%d\\t%s" % (word_sum, current_key)\n        word_sum = 0\n        current_key = key\n    word_sum += count\n\nif current_key:\n    print "%d\\t%s" % (word_sum, current_key)')


# In[5]:


get_ipython().run_cell_magic('writefile', 'reducer2.py', '\nimport sys\n\ncurrent_word = None\nword_sum = 0\n\nfor line in sys.stdin:\n    try:\n        count, word = line.strip().split(\'\\t\', 1)\n        count = int(count)\n    except ValueError as e:\n        continue\n    if current_word != word:\n        if current_word:\n            print "%s\\t%d" % (current_word, word_sum)\n        word_sum = 0\n        current_word = word\n    word_sum += count\n\nif current_word:\n    print "%s\\t%d" % (current_word, word_sum)')


# ## Step 3. Bash commands
# 
# <b> Hint: </b> For printing the exact row you may use basic UNIX commands. For instance, sed/head/tail/... (if you know other commands, you can use them).
# 
# To run both jobs, you must use two consecutive yarn-commands. Remember that the input for the second job is the ouput for the first job.

# In[6]:


get_ipython().run_cell_magic('bash', '', '\nOUT_DIR="assignment1_"$(date +"%s%6N")\nOUT_DIR2="assignment1_b_"$(date +"%s%6N")\n\n# Code for your first job\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -files mapper1.py,reducer1.py,combiner1.py \\\n    -mapper \'python mapper1.py\' \\\n    -combiner \'python combiner1.py\' \\\n    -reducer \'python reducer1.py\' \\\n    -numReduceTasks 2 \\\n    -input /data/wiki/en_articles_part \\\n    -output ${OUT_DIR} > /dev/null\n\n# Code for your second job\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \\\n    -D mapreduce.partition.keycomparator.options="-k1,2nr" \\\n    -files mapper2.py,reducer2.py \\\n    -mapper \'python mapper2.py\' \\\n    -reducer \'python reducer2.py\' \\\n    -numReduceTasks 1 \\\n    -input ${OUT_DIR} \\\n    -output ${OUT_DIR2} > /dev/null\n\n# Code for obtaining the results\nhdfs dfs -cat ${OUT_DIR2}/part-00000 | sed -n \'7p;8q\'\nhdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null\nhdfs dfs -rm -r -skipTrash ${OUT_DIR2}* > /dev/null')

