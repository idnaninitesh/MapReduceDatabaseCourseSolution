#!/bin/sh:
# the input files are in folder /user/cloudera/wordcount/input/*
# the output files from mapreduce are generated in /user/cloudera/wordcount/output/

# steps to run the script
# 1) create the input directory
hadoop fs -mkdir /user/cloudera/wordcount /user/cloudera/wordcount/input

# 2) get the input files and put in input dir
wget http://bmidb.cs.stonybrook.edu/publicdata/big.txt
wget http://textfiles.com/programming/writprog.pro
hadoop fs -put big.txt /user/cloudera/wordcount/input
hadoop fs -put writprog.pro /user/cloudera/wordcount/input

# 3) get the code and stopwords file i.e. wordcount_mapper.py, wordcount_reducer.py and
# wordcount_stopwords.txt should be in the /src folder
mv wordcount_mapper.py wordcount_reducer.py wordcount_stopwords.txt /src/

# 4) remove the output directory if it exists
hadoop fs -rm -r /user/cloudera/wordcount/output

# 5) run wordcount with case sensitive flag set to false
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-2.6.0-cdh5.7.0.jar -D mapred.reduce.tasks=16 -mapper "python /src/wordcount_mapper.py false" -reducer "python /src/wordcount_reducer.py false" -input /user/cloudera/wordcount/input/big.txt,/user/cloudera/wordcount/input/writprog.pro -output /user/cloudera/wordcount/output

# 6) the multiple files from output folder are consolidated in the file 
# wordcount_output in the /src folder
hadoop fs -cat /user/cloudera/wordcount/output/* | sort -k1 > /src/wordcount_output

# 7) replace the false flag by true in step (5) above to check with case sensitiviness enabled
