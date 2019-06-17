#!/usr/bin/env python
"""reducer.py"""

import sys
import string
from datetime import date
from operator import itemgetter

# handle all the required input flags
aggregrator = ''
column = ''

if len(sys.argv) == 5:
	aggregrator = sys.argv[3].lower()
	if aggregrator != 'avg' and aggregrator != 'min' and aggregrator != 'max':
			print 'Invalid arguments for reducer'
			sys.exit()
	column = sys.argv[4].lower()
	if column != 'high' and column != 'low' and column != 'close':
			print 'Invalid arguments for reducer'
			sys.exit()
else:
	print 'Invalid arguments for reducer'
	sys.exit()


current_ticker = None
current_count = 1
current_val = 0.0
ticker = None

# input comes from STDIN
for line in sys.stdin:
	# remove leading and trailing whitespace
	line = line.strip()
	# parse the input we got from mapper.py
	try:
		ticker, val = line.split('\t', 1)
	except ValueError:
		# some values from mapper do not have
		# a valid price value hence ignore them
		continue
	
	# convert val (currently a string) to int
	try:
		val = float(val)
	except ValueError:
		# count was not a number, so silently
		# ignore/discard this line
		continue

	# this IF-switch only works because Hadoop sorts map output
	# by key (here: ticker) before it is passed to the reducer
	if current_ticker == ticker:
		if aggregrator == 'min':
			current_val = min(current_val, val)
		elif aggregrator == 'max':
			current_val = max(current_val, val)
		else:
 			current_val += val
			current_count += 1
	else:
		if current_ticker:
			# if 'avg' then divide by count
			if aggregrator == 'avg':
				current_val = current_val/current_count
			current_val = round(current_val, 2)
			# write result to STDOUT
			print '%s\t%s' % (current_ticker, current_val)
		current_val = val
		current_ticker = ticker
		current_count = 1

# do not forget to output the last ticker if needed!
if current_ticker == ticker:
	# if 'avg' then divide by count
	if aggregrator == 'avg':
		current_val = current_val/current_count
	current_val = round(current_val, 1)
	print '%s\t%s' % (current_ticker, current_val)
