#!/usr/bin/env python
"""mapper.py"""

import sys
import string

# 0-ticker,1-date,2-open,3-high,4-low,5-close,volume,ex-dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume

# input comes from STDIN (standard input)
for line in sys.stdin:
	# seperate data by column to get a list
	line = line.strip().split(',')
	# get the required fields i.e. year, ticker, low and high value
	year = line[1].split('-')[0]
	ticker = line[0]
	low = line[4]
	high = line[3]
	# write the results to STDOUT (standard output);
	# what we output here will be the input for the
	# Reduce step, i.e. the input for reducer.py
	print '%s\t%s\t%s\t%s' % (year, ticker, low, high)
