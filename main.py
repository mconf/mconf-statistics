#! /usr/bin/python

import sys, os
from LogLineEvent import *
from StatTable import StatTable

if len(sys.argv) < 3:
    print "usage: " + sys.argv[0] + " [logfile] [datafile]"
    print "where [logfile] is the latest bigbluebutton log file and"
    print "      [datafile] is the file for the output data"
    sys.exit(0)

events = []

parse(sys.argv[1], events)

statTable = StatTable(sys.argv[2])
statTable.update(events)
