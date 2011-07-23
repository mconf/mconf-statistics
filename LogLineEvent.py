import re
import dateutil.parser
import calendar

def parse_user_join(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.USER_JOIN
    result.__id__ = regex_match.group("user_id")
    return result

def parse_user_leave(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.USER_LEAVE
    result.__id__ = regex_match.group("user_id")
    return result

def parse_user_name(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.USER_NAME
    result.__id__ = regex_match.group('user_id')
    result.__username__ = regex_match.group('user_name')
    return result

def parse_room_create(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.ROOM_CREATE
    result.__id__ = regex_match.group("room_id")
    return result

def parse_room_destroy(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.ROOM_DESTROY
    result.__id__ = regex_match.group("room_id")
    return result

def parse_video_start(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.VIDEO_START
    result.__id__ = regex_match.group("user_id")
    return result

def parse_video_stop(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.VIDEO_STOP
    result.__id__ = regex_match.group("user_id")
    return result

def parse_audio_start(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.AUDIO_START
    result.__id__ = regex_match.group("room_id")
    result.__username__ = regex_match.group("user_name")
    return result

def parse_audio_id(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.AUDIO_ID
    result.__id__ = regex_match.group('audio_id')
    result.__username__ = regex_match.group('user_name')
    return result

def parse_audio_stop(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.AUDIO_STOP
    result.__id__ = regex_match.group("audio_id")
    return result
    
def parse_server_restarted(line, regex_match):
    result = LogLineEvent(line)
    result.__type__ = LogLineEvent.SERVER_RESTARTED
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
        USER_JOIN
        USER_LEAVE
        AUDIO_START
        AUDIO_STOP
        VIDEO_START
        VIDEO_STOP
        ROOM_CREATE
        ROOM_DESTROY
        SERVER_RESTARTED

    __timestamp__ contains a timestamp that identifies the event in time, and __id__
    contains an identifier for the user or room that originated the event.
    """
    ## each of these regular expressions handles one specific event
    regexes = {
        re.compile(".*INFO  o.b.c.BigBlueButtonApplication - \[clientid=(?P<user_id>.*)\] connected.*") : parse_user_join,
        ### YES! bigbluebutton cannot spell
        re.compile(".*INFO  o.b.c.BigBlueButtonApplication - \[clientid=(?P<user_id>.*)\] disconnnected.*") : parse_user_leave,
        re.compile(".*DEBUG o.b.c.BigBlueButtonApplication - User \[userid=(?P<user_id>.*),username=(?P<user_name>.*),role.*") : parse_user_name,
        re.compile(".*INFO  o.b.c.s.p.ParticipantsApplication - Creating room (?P<room_id>.*)") : parse_room_create,
        re.compile(".*INFO  o.b.c.s.p.ParticipantsApplication - Destroying room (?P<room_id>.*)") : parse_room_destroy,
        re.compile(".*DEBUG o.b.conference.RoomsManager - Change participant status (?P<user_id>.*) - hasStream \[true\]") : parse_video_start,
        re.compile(".*DEBUG o.b.conference.RoomsManager - Change participant status (?P<user_id>.*) - hasStream \[false\]") : parse_video_stop,
        re.compile(".*DEBUG o.b.w.red5.voice.ClientManager - Participant (?P<user_name>.*)joining room (?P<room_id>.*)") : parse_audio_start,
        re.compile(".*DEBUG o.b.w.voice.internal.RoomManager - Joined \[(?P<audio_id>.*),(?P<user_name>.*),.*,.*\]*") : parse_audio_id,
        re.compile(".*DEBUG o.b.w.red5.voice.ClientManager - Participant \[(?P<audio_id>.*),.*\] leaving") : parse_audio_stop,
        re.compile(".*DEBUG ROOT - Starting up context bigbluebutton") : parse_server_restarted
    }

    USERS        = 'users_count'
    USER_JOIN    = 'user_join'
    USER_LEAVE   = 'user_leave'
    USER_NAME    = 'user_name'

    AUDIO        = 'audio_count'
    AUDIO_ID     = 'audio_id'
    AUDIO_START  = 'audio_start'
    AUDIO_STOP   = 'audio_stop'

    VIDEO        = 'video_count'
    VIDEO_START  = 'video_start'
    VIDEO_STOP   = 'video_stop'
    
    ROOM         = 'room_count'
    ROOM_CREATE  = 'room_create'
    ROOM_DESTROY = 'room_destroy'

    SERVER           = 'server'
    SERVER_RESTARTED = 'server_restarted'

    EventTypeMap = {
        USER_JOIN: USERS,
        USER_LEAVE: USERS,
        USER_NAME: USERS,

        AUDIO_START: AUDIO,
        AUDIO_STOP: AUDIO,
        AUDIO_ID: AUDIO,

        VIDEO_START: VIDEO,
        VIDEO_STOP: VIDEO,

        ROOM_CREATE: ROOM,
        ROOM_DESTROY: ROOM,
        
        SERVER_RESTARTED: SERVER
    }

    EventTypeNames = {
        USER_JOIN: 'USER_JOIN',
        USER_LEAVE: 'USER_LEAVE',
        USER_NAME: 'USER_NAME',
        AUDIO_START: 'AUDIO_START',
        AUDIO_STOP: 'AUDIO_STOP',
        AUDIO_ID: 'AUDIO_ID',
        VIDEO_START: 'VIDEO_START',
        VIDEO_STOP: 'VIDEO_STOP',
        ROOM_CREATE: 'ROOM_CREATE',
        ROOM_DESTROY: 'ROOM_DESTROY',
        SERVER_RESTARTED: 'SERVER_RESTARTED'
    }

    def __init__(self, line):
        tokens = line.split(" ",2)
        datetime = " ".join(tokens[0:2]).replace(',','.')
        self.__timestamp__ = calendar.timegm(dateutil.parser.parse(datetime).timetuple())

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
