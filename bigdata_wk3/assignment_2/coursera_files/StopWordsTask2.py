
# coding: utf-8

# # Hadoop Streaming assignment 2: Stop Words

# The purpose of this task is to improve the previous "Word rating" program. You have to calculate how many stop words are there in the input dataset. Stop words list is in `/datasets/stop_words_en.txt` file. 
# 
# Use Hadoop counters to compute the number of stop words and total words in the dataset. The result is the percentage of stop words in the entire dataset (without percent symbol).
# 
# There are several points for this task:
# 
# 1) As an output, you have to get the percentage of stop words in the entire dataset without percent symbol (correct answer on sample dataset is `41.603`).
# 
# 2) As you can see in the Hadoop Streaming userguide "you will need to use `-files` option to tell the framework to pack your executable files as a part of a job submission."
# 
# 3) Do not forget to redirect junk output to `/dev/null`.
# 
# 4) You may modify mappers/reducers from "Word rating" task and parse its output to get the answer on "Stop Words" task.
# 
# 5) You may use mapper/reducer to get `"Stop Words"` and `"Total Words"` amounts and redirect them to sys.stderr. After that you may redirect the output of MapReduce to the parsed function. In this function you may find rows correspond to these amounts and compute the percentage.
# 
# Here you can find the draft for the main steps of the task. You can use other methods to get the solution.

# ## Step 1. Create the mapper.
# 
# <b>Hint:</b> Create the mapper, which calculates Total word and Stop word amounts. You may redirect this information to sys.stderr. This will make it possible to parse these data on the next steps.
# 
# Example of the redirections:
# 
# `print >> sys.stderr, "reporter:counter:Wiki stats,Total words,%d" % count`
# 
# Remember about the Distributed cache. If we add option `-files mapper.py,reducer.py,/datasets/stop_words_en.txt`, then `mapper.py, reducer.py` and `stop_words_en.txt` file will be in the same directory on the datanodes. Hence, it is necessary to use a relative path `stop_words_en.txt` from the mapper to access this txt file.

# In[1]:


get_ipython().run_cell_magic('writefile', 'mapper.py', '\n\nimport sys\nimport re\n\n\nreload(sys)\nsys.setdefaultencoding(\'utf-8\')  # required to convert to unicode\n\npath = \'stop_words_en.txt\'\n\nstop_words = set()\nstop_word_file = open(path, \'r\')\nfor stop_word in stop_word_file:\n    stop_words.add(stop_word.strip())\n\n# print(stop_words)\nfor line in sys.stdin:\n    try:\n        article_id, text = unicode(line.strip()).split(\'\\t\', 1)\n    except ValueError as e:\n        continue\n\n    words = re.split("\\W*\\s+\\W*", text, flags=re.UNICODE)\n\n    stop_word_count = 0\n    word_count = 0\n\n    for word in words:\n        word_count += 1\n        if word.lower() in stop_words:\n            stop_word_count += 1\n    \n    print "%d\\t%d" % (word_count, stop_word_count)')


# ## Step 2. Create the reducer.
# 
# Create the reducer, which will accumulate the information after the mapper step. You may implement the combiner if you want. It can be useful from optimizing and speed up your computations (see the lectures from the Week 2 for more details).

# In[2]:


get_ipython().run_cell_magic('writefile', 'reducer.py', '\nimport sys\n\nword_sum = 0\nstop_word_sum = 0\n\nfor line in sys.stdin:\n    try:\n        word_count, stop_word_count = line.strip().split(\'\\t\', 1)\n        word_count = int(word_count)\n        stop_word_count = int(stop_word_count)\n    except ValueError as e:\n        continue\n    word_sum += word_count\n    stop_word_sum += stop_word_count\n\nprint "%d\\t%d" % (word_sum, stop_word_sum)\nprint >> sys.stderr, "reporter:counter:Personal Counters,Total words,%d" % word_sum\nprint >> sys.stderr, "reporter:counter:Personal Counters,Stop words,%d" % stop_word_sum')


# ## Step 3. Create the parsed function.
# 
# <b>Hint:</b> Create the function, which will parse MapReduce sys.stderr for Total word and Stop word amounts.
# 
# The `./counter_process.py` script should do the following:
# 
# - parse hadoop logs from Stderr,
# 
# - retrieve values of 2 user-defined counters,
# 
# - compute percentage and output it into the stdout.

# In[3]:


get_ipython().run_cell_magic('writefile', 'counter_process.py', '\n#! /usr/bin/env python\n\nimport sys\nimport re\n\nif __name__ == \'__main__\':\n    \n    word_count = 0\n    stop_word_count = 0\n\n    for line in sys.stdin:\n        total_word_match = re.search(str(sys.argv[1]) + \'=(\\d+)\', line)\n        if total_word_match:\n            word_count = int(total_word_match.group(1))\n        \n        stop_word_match = re.search(str(sys.argv[2]) + \'=(\\d+)\', line)\n        if stop_word_match:\n            stop_word_count = int(stop_word_match.group(1))\n\n    stop_word_percentage = float(stop_word_count) / float(word_count) * 100\n\n    print "%.3f" % (stop_word_percentage)')


# ## Step 4. Bash commands
# 
# <b> Hints: </b> 
# 
# 1) If you want to redirect standard output to txt file you may use the following argument in yarn jar:
# 
# ```
# yarn ... \
#   ... \
#   -output ${OUT_DIR} > /dev/null 2> $LOGS
# ```
# 
# 2) For printing the percentage of stop words in the entire dataset you may parse the MapReduce output. Parsed script may be written in Python code. 
# 
# To get the result you may use the UNIX pipe operator `|`. The output of the first command acts as an input to the second command (see lecture file-content-exploration-2 for more details).
# 
# With this operator you may use command `cat` to redirect the output of MapReduce to ./counter_process.py with arguments, which correspond to the `"Stop words"` and `"Total words"` counters. Example is the following:
# 
# `cat $LOGS | python ./counter_process.py "Stop words" "Total words"`
# 
# Now something about Hadoop counters naming. 
#  - Built-in Hadoop counters usually have UPPER_CASE names. To make the grading system possible to distinguish your custom counters and system ones please use the following pattern for their naming: `[Aa]aaa...` (all except the first letters should be in lowercase);
#  - Another points is how Hadoop sorts the counters. It sorts them lexicographically. Grading system reads your first counter as Stop words counter and the second as Total words. Please name you counters in such way that Hadoop set the Stop words counter before the Total words. 
#  
# E.g. "Stop words" and "Total words" names are Ok because they correspond both requirements.
# 
# 3) In Python code sys.argv is a list, which contains the command-line arguments passed to the script. The name of the script is in `sys.argv[0]`. Other arguments begin from `sys.argv[1]`.
# 
# Hence, if you have two arguments, which you send from the Bash to your python script, you may use arguments in your script with the following command:
# 
# `function(sys.argv[1], sys.argv[2])`
# 
# 4) Do not forget about printing your MapReduce output in the last cell. You may use the next command:
# 
# `cat $LOGS >&2`

# In[4]:


get_ipython().run_cell_magic('bash', '', '\nOUT_DIR="coursera_mr_task2"$(date +"%s%6N")\nNUM_REDUCERS=8\nLOGS="stderr_logs.txt"\n\n# Stub code for your job\n\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -files mapper.py,reducer.py,counter_process.py,/datasets/stop_words_en.txt,${LOGS} \\\n    -mapper "/usr/bin/python2 mapper.py" \\\n    -reducer "/usr/bin/python2 reducer.py" \\\n    -numReduceTasks ${NUM_REDUCERS} \\\n    -input /data/wiki/en_articles_part \\\n    -output ${OUT_DIR} > /dev/null 2> ${LOGS}\n\n\ncat ${LOGS} | /usr/bin/python2 ./counter_process.py "Total words" "Stop words"\ncat ${LOGS} >&2\n\nhdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null')

