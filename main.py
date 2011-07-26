import sys, os
from LogLineEvent import parse

if len(sys.argv) < 2:
    print "usage: " + sys.argv[0] + " [logfile]"
    print "where [logfile] is the latest bigbluebutton log file"
    sys.exit(0)

events = []

parse(sys.argv[1], events)

## updates the daily table

## updates the weekly table

## updates the monthly table

print [event.timestamp() for event in events]
