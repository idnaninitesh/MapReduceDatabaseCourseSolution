#!/bin/sh:
# the input files are in folder /user/cloudera/stockanalysis/input/*
# the output files from mapreduce are generated in /user/cloudera/stockanalysis/output/

# steps to run the script
# 1) create the input directory
hadoop fs -mkdir /user/cloudera/stockanalysis /user/cloudera/stockanalysis/input

# 2) get the input files and put in input dir
hadoop fs -put stockdatafull.csv /user/cloudera/stockanalysis/input/

# 3) get the code i.e. stockanalysis_mapper.py, stockanalysis_reducer.py
# should be in the /src folder
mv stockanalysis_mapper.py stockanalysis_reducer.py /src/

# 4) remove the output directory if it exists
hadoop fs -rm -r /user/cloudera/stockanalysis/output

# 5) run stockanalysis with some sample arguments as follows
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-2.6.0-cdh5.7.0.jar -D mapred.reduce.tasks=16 -mapper "python /src/stockanalysis_mapper.py 10/11/1990 12/09/2013 avg low" -reducer "python /src/stockanalysis_reducer.py 10/11/1990 12/09/2013 avg low" -input /user/cloudera/stockanalysis/input/stockdatafull.csv -output /user/cloudera/stockanalysis/output

# 6) the multiple files from output folder are consolidated in the file 
# stockanalysis_output in the /src folder
hadoop fs -cat /user/cloudera/stockanalysis/output/* | sort -k1 > /src/stockanalysis_output

# 7) replace the dates, aggregrator or value filed in step (5) above to check for different cases
