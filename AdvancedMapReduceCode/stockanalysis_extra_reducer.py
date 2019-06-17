#!/usr/bin/env python
"""reducer.py"""

import sys
import string
from datetime import date
from operator import itemgetter


current_year = None
year = None
max_ticker = None
max_low = 0.0
max_high = 0.0
max_diff = 0.0

# input comes from STDIN
for line in sys.stdin:
	# remove leading and trailing whitespace
	line = line.strip()
	# parse the input we get from mapper.py
	try:
		year, ticker, low, high = line.split('\t', 3)
	except ValueError:
		# some values from mapper do not have
		# a valid value hence ignore them
		continue

	# convert low, high (currently a string) to float
	try:
		low = float(low)
		high = float(high)
		diff = high-low
	except ValueError:
		# count was not a number, so silently
		# ignore/discard this line
		continue

	# this IF-switch only works because Hadoop sorts map output
	# by key (year and ticker) before it is passed to the reducer
	if current_year == year:
		if diff > max_diff:
			max_diff = diff
			max_low = low
			max_high = high
			max_ticker = ticker
	else:
		if current_year:
			# write result to STDOUT
			max_low = round(max_low, 2)
			max_high = round(max_high, 2)
			max_diff = round(max_diff, 2)
			print '%s\t%s\t%s\t%s\t%s' % (current_year, max_ticker, max_low, max_high, max_diff)
		current_year = year
		current_ticker = ticker
		max_ticker = ticker
		max_low = low
		max_high = high
		max_diff = high-low

# do not forget to output the last year if needed!
if current_year == year:
	# write result to STDOUT
	max_low = round(max_low, 2)
	max_high = round(max_high, 2)
	max_diff = round(max_diff, 2)
	print '%s\t%s\t%s\t%s\t%s' % (current_year, max_ticker, max_low, max_high, max_diff)
