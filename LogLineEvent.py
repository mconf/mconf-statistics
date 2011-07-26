import re
import dateutil.parser
import time

def parse_user_join(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_USER_JOIN
    result.__id__ = regex_match.groups("user_id")[0]
    return result

def parse_user_leave(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_USER_LEAVE
    result.__id__ = regex_match.groups("user_id")[0]
    return result

def parse_room_create(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_ROOM_CREATE
    result.__id__ = regex_match.groups("room_id")[0]
    return result

def parse_room_destroy(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_ROOM_DESTROY
    result.__id__ = regex_match.groups("room_id")[0]
    return result

def parse(filename, events=None):
    f = open(filename, 'r')
    lines = f.readlines()
    f.close()

    if events is None:
        events = []

    for line in lines:
        for regex in LogLineEvent.regexes:
            match = regex.match(line)
            if match: 
                events.append(LogLineEvent.regexes[regex](line, match))
                
    return events

class LogLineEvent:
    """
    LogLineEvent is the base class for all event classes that handle
    single lines of the BigBlueButton log. It contains three important
    fields: self.__type__, self.__timestamp__ and self.__id__. __type__ is one
    of:
        LOG_LINE_EVENT_USER_JOIN
        LOG_LINE_EVENT_USER_LEAVE
        LOG_LINE_EVENT_AUDIO_START
        LOG_LINE_EVENT_AUDIO_STOP
        LOG_LINE_EVENT_VIDEO_START
        LOG_LINE_EVENT_VIDEO_STOP
        LOG_LINE_EVENT_ROOM_CREATE
        LOG_LINE_EVENT_ROOM_DESTROY

    __timestamp__ contains a timestamp that identifies the event in time, and __id__
    contains an identifier for the user or room that originated the event.
    """
    ## each of these regular expressions handles one specific event
    global parse_user_join, parse_user_leave
    regexes = {
        re.compile(".*\[clientid=(?P<user_id>.*)\] connected.*") : parse_user_join,
        ### YES! bigbluebutton cannot spell - jackasses
        re.compile(".*\[clientid=(?P<user_id>.*)\] disconnnected.*") : parse_user_leave,
        re.compile(".*Adding room (?P<room_id>.*)") : parse_room_create,
        re.compile(".*Remove room (?P<room_id>.*)") : parse_room_destroy,
    }

    LOG_LINE_EVENT_USER_JOIN = 0
    LOG_LINE_EVENT_USER_LEAVE = 1
    LOG_LINE_EVENT_AUDIO_START = 2
    LOG_LINE_EVENT_AUDIO_STOP = 3
    LOG_LINE_EVENT_VIDEO_START = 4
    LOG_LINE_EVENT_VIDEO_STOP = 5
    LOG_LINE_EVENT_ROOM_CREATE = 6
    LOG_LINE_EVENT_ROOM_DESTROY = 7

    def __init__(self, line):
        tokens = line.split(" ",2)
        datetime = " ".join(tokens[0:2]).replace(',','.')
        self.__timestamp__ = time.mktime(dateutil.parser.parse(datetime).timetuple())

    def type(self):
        return self.__type__

    def timestamp(self):
        return self.__timestamp__

    def id(self):
        return self.__id__
