import sys, os
from LogLineEvent import *
from StatTable import StatTable

if len(sys.argv) < 2:
    print "usage: " + sys.argv[0] + " [logfile]"
    print "where [logfile] is the latest bigbluebutton log file"
    sys.exit(0)

events = []

parse(sys.argv[1], events)

statTable = StatTable({'daily': './daily.log', 'weekly': './weekly.log','monthly': './monthly.log'},
                      (LogLineEvent.LOG_LINE_EVENT_USER_JOIN,
                       LogLineEvent.LOG_LINE_EVENT_USER_LEAVE))
statTable.update(events)
