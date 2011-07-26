import re
from log_entry import BBBLogEntry

date_regex = re.compile('.{4}-.{2}-.{2}')

def add_log_entry(log, log_entry):
    if log.has_key(log_entry._type):
        log[log_entry._type].append(log_entry)
    else:
        log[log_entry._type] = [log_entry]

def parse(filename, logs=None):
    f = open(filename, 'r')
    lines = f.readlines()
    f.close()

    if logs is None:
        logs = {}

    lines = list(lines)
    line = lines[0]
    for l in lines[1:]:
        if re.match(date_regex, l):
        ## the new line is the start of a new log message - process
        ## the previous one and reset
            log = BBBLogEntry(line)
            add_log_entry(logs, log)
            line = l
        else:
        ## the new line is still part of the previous log message - just append them
            line += ' ' + l

    if re.match(date_regex, line):
    ## the last line is a log message - this should techincally always happen
        log = BBBLogEntry(line)
        add_log_entry(logs, log)
    else:
    ## this should never happen - just a sanity check
        print 'last line not a log message?'

    return logs
