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

def parse_user_name(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_USER_NAME
    result.__id__ = regex_match.groups("user_id")[0]
    result.__username__ = regex_match.groups("user_name")[0]
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

def parse_video_start(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_VIDEO_START
    result.__id__ = regex_match.groups("user_id")[0]
    return result

def parse_video_stop(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_VIDEO_STOP
    result.__id__ = regex_match.groups("user_id")[0]
    return result

def parse_audio_start(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_AUDIO_START
    result.__id__ = regex_match.groups("room_id")[0]
    result.__username__ = regex_match.groups("user_name")[0]
    return result

def parse_audio_id(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_AUDIO_ID
    result.__id__ = regex_match.groups("audio_id")[0]
    result.__username__ = regex_match.groups("user_name")[0]
    return result

def parse_audio_stop(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.LOG_LINE_EVENT_AUDIO_STOP
    result.__id__ = regex_match.groups("room_id")[0]
    return result
 
def parse(filename, events=None):
    try:
        f = open(filename, 'r')
        lines = f.readlines()
        f.close()
    except:
        lines = []

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
    regexes = {
        re.compile(".*INFO  o.b.c.BigBlueButtonApplication - \[clientid=(?P<user_id>.*)\] connected.*") : parse_user_join,
        ### YES! bigbluebutton cannot spell
        re.compile(".*INFO  o.b.c.BigBlueButtonApplication - \[clientid=(?P<user_id>.*)\] disconnnected.*") : parse_user_leave,
        re.compile(".*DEBUG o.b.c.BigBlueButtonApplication - User \[userid=(?P<user_id>.*),username=(?P<user_name>.*),*") : parse_user_name,
        re.compile(".*INFO  o.b.c.s.p.ParticipantsApplication - Creating room (?P<room_id>.*)") : parse_room_create,
        re.compile(".*INFO  o.b.c.s.p.ParticipantsApplication - Destroying room (?P<room_id>.*)") : parse_room_destroy,
        re.compile(".*DEBUG o.b.conference.RoomsManager - Change participant status (?P<user_id>.*) - hasStream \[true\]") : parse_video_start,
        re.compile(".*DEBUG o.b.conference.RoomsManager - Change participant status (?P<user_id>.*) - hasStream \[false\]") : parse_video_stop,
        re.compile(".*DEBUG o.b.w.red5.voice.ClientManager - Participant (?P<user_name>.*)joining room (?P<room_id>.*)") : parse_audio_start,
        re.compile(".*DEBUG o.b.w.voice.internal.RoomManager - Joined \[(?P<audio_id>.*),(?P<user_name>.*),*") : parse_audio_id,
        re.compile(".*DEBUG o.b.w.red5.voice.ClientManager - Participant \[.*,(?P<room_id>.*)\] leaving") : parse_audio_stop,
    }

    LOG_LINE_EVENT_USERS        = 'users'
    LOG_LINE_EVENT_USER_JOIN    = 'user_join'
    LOG_LINE_EVENT_USER_LEAVE   = 'user_leave'
    LOG_LINE_EVENT_USER_NAME    = 'user_name'

    LOG_LINE_EVENT_AUDIO        = 'audio'
    LOG_LINE_EVENT_AUDIO_ID     = 'audio_id'
    LOG_LINE_EVENT_AUDIO_START  = 'audio_start'
    LOG_LINE_EVENT_AUDIO_STOP   = 'audio_stop'

    LOG_LINE_EVENT_VIDEO        = 'video'
    LOG_LINE_EVENT_VIDEO_START  = 'video_start'
    LOG_LINE_EVENT_VIDEO_STOP   = 'video_stop'
    
    LOG_LINE_EVENT_ROOM         = 'room'
    LOG_LINE_EVENT_ROOM_CREATE  = 'room_create'
    LOG_LINE_EVENT_ROOM_DESTROY = 'room_destroy'

    EventTypeMap = {
        LOG_LINE_EVENT_USER_JOIN: LOG_LINE_EVENT_USERS,
        LOG_LINE_EVENT_USER_LEAVE: LOG_LINE_EVENT_USERS,
        LOG_LINE_EVENT_USER_NAME: LOG_LINE_EVENT_USERS,
        
        LOG_LINE_EVENT_AUDIO_START: LOG_LINE_EVENT_AUDIO,
        LOG_LINE_EVENT_AUDIO_STOP: LOG_LINE_EVENT_AUDIO,
        LOG_LINE_EVENT_AUDIO_ID: LOG_LINE_EVENT_AUDIO,

        LOG_LINE_EVENT_VIDEO_START: LOG_LINE_EVENT_VIDEO,
        LOG_LINE_EVENT_VIDEO_STOP: LOG_LINE_EVENT_VIDEO,

        LOG_LINE_EVENT_ROOM_CREATE: LOG_LINE_EVENT_ROOM,
        LOG_LINE_EVENT_ROOM_DESTROY: LOG_LINE_EVENT_ROOM
    }

    EventTypeNames = {
        LOG_LINE_EVENT_USER_JOIN: 'USER_JOIN',
        LOG_LINE_EVENT_USER_LEAVE: 'USER_LEAVE',
        LOG_LINE_EVENT_USER_NAME: 'USER_NAME',
        LOG_LINE_EVENT_AUDIO_START: 'AUDIO_START',
        LOG_LINE_EVENT_AUDIO_STOP: 'AUDIO_STOP',
        LOG_LINE_EVENT_AUDIO_ID: 'AUDIO_ID',
        LOG_LINE_EVENT_VIDEO_START: 'VIDEO_START',
        LOG_LINE_EVENT_VIDEO_STOP: 'VIDEO_STOP',
        LOG_LINE_EVENT_ROOM_CREATE: 'ROOM_CREATE',
        LOG_LINE_EVENT_ROOM_DESTROY: 'ROOM_DESTROY'
    }

    def __init__(self, line):
        tokens = line.split(" ",2)
        datetime = " ".join(tokens[0:2]).replace(',','.')
        self.__timestamp__ = time.mktime(dateutil.parser.parse(datetime).timetuple())

    def __str__(self):
        return LogLineEvent.EventTypeNames[self.__type__]

    def __repr__(self):
        return self.__str__()

    def type(self):
        return self.__type__

    def timestamp(self):
        return self.__timestamp__

    def id(self):
        return self.__id__

    def username(self):
        return self.__username__
