#!/usr/bin/env python
"""mapper.py"""

import sys
import string
from datetime import date

# handle all the required input flags
dateList = None
startDate = None
endDate = None
aggregrator = ''
column = ''
column_num = -1

if len(sys.argv) == 5:
	dateList = sys.argv[1].split('/')	# mm/dd/yyyy
	startDate = date(int(dateList[2]), int(dateList[0]), int(dateList[1]))
	dateList = sys.argv[2].split('/')	# mm/dd/yyyy
	endDate = date(int(dateList[2]), int(dateList[0]), int(dateList[1]))
	aggregrator = sys.argv[3].lower()
	if aggregrator != 'avg' and aggregrator != 'min' and aggregrator != 'max':
		print 'Invalid arguments for mapper'
		sys.exit()
	column = sys.argv[4].lower()
	if column != 'high' and column != 'low' and column != 'close':
		print 'Invalid arguments for mapper'
		sys.exit()
	if column == 'high':
		column_num = 3
	elif column == 'low':
		column_num = 4
	else:
		column_num = 5
else:
	print 'Invalid arguments for mapper'
	sys.exit()


# input comes from STDIN (standard input)
for line in sys.stdin:
	# seperate data by column and get date value
	line = line.strip().split(',')
	dateList = line[1].split('-')
	# validate the date (between start and end date)
	currDate = date(int(dateList[0]), int(dateList[1]), int(dateList[2]))
	if not startDate <= currDate <= endDate:
		continue
	# get the ticker and required column value
	ticker = line[0]
	val = line[column_num]
	# write the results to STDOUT (standard output);
	# what we output here will be the input for the
	# Reduce step, i.e. the input for reducer.py
	print '%s\t%s' % (ticker, val)
