import time
import os
import json
import datetime
import calendar

import Constants
from LogLineEvent import LogLineEvent
    
class StatTable:
    """
    StatTable is the class that interfaces with the files containing the stats
    for important bigbluebutton events. The main method is update(events), which loads
    the current table from file (a json-encoded file) and adds the events that haven't been
    accounted for yet, doing proper aggregation.
    """

    STAT_TABLE_SIZES = {
        'daily':   1440, # 1/min for 24h
        'weekly':  1008, # 1/(10 min) for 7 days
        'monthly': 1440  # 1/(30 min) for 30 days
        }

    STAT_AGGREGATION_SIZES = {
        'daily': 1,
        'weekly': 10,
        'monthly': 30
        }

    def __init__(self, filename):
        self.__filename__ = filename

        cmd = 'touch ' + self.__filename__
        os.system(cmd)

        self.__data__ = self.__readFile__()

    def __writeFile__(self):
        # writes the data as a JSON-encoded dict
        f = open(self.__filename__, 'w')

        filestring = json.dumps(self.__data__) + '\n'
        f.write(filestring)
        f.close()

    def __readFile__(self):
        # reads a JSON-encoded object from file
        f = open(self.__filename__, 'r')

        # default object for empty files
        obj = {
            'daily'  : {'datapoints': []},
            'weekly' : {'datapoints': []},
            'monthly': {'datapoints': []}
        }

        try:
            obj = json.loads(f.read())
        except ValueError as err:
            pass

        f.close()

        return obj

    def datapoints(self):
        return self.__datapoints__

    def __slideWindow__(self):
        """
        reads the files and deletes excessive lines from the start,
        to maintain the max allowable size for each file
        """
        for key in ['daily', 'weekly', 'monthly']:
            if len(self.__data__[key]['datapoints']) > StatTable.STAT_TABLE_SIZES[key]:
                self.__data__[key]['datapoints'] =\
                    self.__data__[key]['datapoints'][len(self.__data__[key]['datapoints']) - StatTable.STAT_TABLE_SIZES[key]:]
            
    def __append__(self, events, latest=None):
        empty_value = { LogLineEvent.USERS: 0, LogLineEvent.AUDIO: 0, LogLineEvent.VIDEO: 0, LogLineEvent.ROOM: 0,
                           'users': {}, 'usernames': {}, 'audio_ids': {}}
        if not latest:
            latest = { 'timestamp': events[0].timestamp() - 1, 'idx': -1,
                'value': dict(empty_value) }
        
        curr_time = latest['timestamp'] + Constants.SECONDS_IN_MIN
        datapoint_idx = latest['idx'] + 1
        counters = dict(latest['value'])

        final_time = calendar.timegm(datetime.datetime.today().timetuple())
        print datetime.datetime.utcfromtimestamp(final_time).ctime()
        events_idx = 0

        increments = {
            LogLineEvent.USER_JOIN: 1, LogLineEvent.USER_LEAVE: -1, LogLineEvent.AUDIO_START: 1, LogLineEvent.AUDIO_STOP: -1,
            LogLineEvent.VIDEO_START: 1, LogLineEvent.VIDEO_STOP: -1, LogLineEvent.ROOM_CREATE: 1, LogLineEvent.ROOM_DESTROY: -1
        }

        while curr_time < final_time:
            # take all events in the list whose timestamp is LESS than curr_time
            # but MORE than latest.timestamp
            while events_idx < len(events):
                event = events[events_idx]
                events_idx += 1

                if event.timestamp() <= latest['timestamp']: continue # we saw this event already
                if event.timestamp() >= curr_time:
                    # this event is for the next minute
                    events_idx -= 1 
                    break

                ## this event is in this minute-window, and hasn't been processed yet
                event_type = LogLineEvent.EventTypeMap[event.type()]

                ## specific event handling
                if event.type() == LogLineEvent.USER_JOIN:
                    ## the user is joining, so we add him/her to the persistent list of users
                    try: counters['users'][event.id()] = { 'audio': False, 'video': False }
                    except KeyError as detail: print 'Handling exception: ', detail
                    
                elif event.type() == LogLineEvent.USER_NAME:
                    ## the user is being named, we must track this name for the audio start/stop events
                    try: counters['usernames'][event.username()] = event.id()
                    except KeyError as detail: print 'Handling exception: ', detail

                elif event.type() == LogLineEvent.USER_LEAVE:
                    ## the user is leaving, so we must decrement the audio/video counters
                    ## if the audio/video streams are true, and delete the entry
                    ## from the users dictionary in the datapoints
                    try:
                        counters[LogLineEvent.AUDIO] -= 1 if counters['users'][event.id()]['audio'] else 0
                        counters[LogLineEvent.VIDEO] -= 1 if counters['users'][event.id()]['video'] else 0
                        ## remove the username from the usernames dictionary
                        candidates = [k for k, v in counters['usernames'].iteritems() if v == event.id()]
                        ## if there are two users with the same username, after the first one leaves
                        ## the candidates will be empty
                        if len(candidates) > 0:
                            del counters['usernames'][candidates[0]]
                        del counters['users'][event.id()]
                    except KeyError as detail: print 'Handling exception: ', detail

                ## start/stop video
                elif event.type() == LogLineEvent.VIDEO_START:
                    try: counters['users'][event.id()]['video'] = True
                    except KeyError as detail: print 'Handling exception: ', detail
                elif event.type() == LogLineEvent.VIDEO_STOP:
                    try: counters['users'][event.id()]['video'] = False
                    except KeyError as detail: print 'Handling exception: ', detail

                ## start/stop audio
                elif event.type() == LogLineEvent.AUDIO_ID:
                    ## we acquire the audio id for the user
                    try: counters['audio_ids'][event.id()] = counters['usernames'][event.username()]
                    except KeyError as detail: print 'Handling exception: ', detail

                elif event.type() == LogLineEvent.AUDIO_START:
                    try:
                        user_id = counters['usernames'][event.username()]
                        counters['users'][event.id()]['audio'] = True
                    except KeyError as detail: print 'Handling exception: ', detail

                elif event.type() == LogLineEvent.AUDIO_STOP:
                    try:
                        user_id = counters['audio_ids'][event.id()]
                        ## sometimes AUDIO_STOP comes after USER_LEAVE
                        if counters['users'].has_key(user_id):
                            counters['users'][event.id()]['audio'] = False
                    except KeyError as detail: print 'Handling exception: ', detail
                
                elif event.type() == LogLineEvent.SERVER_RESTARTED:
                    counters = dict(empty_value)
               
                ## we skip some of the control events
                if event.type() in [LogLineEvent.USER_NAME, LogLineEvent.AUDIO_ID, LogLineEvent.SERVER_RESTARTED]: continue

                try: counters[event_type] += increments[event.type()]
                except KeyError as detail: print 'Handling exception: ', detail

            self.__data__['daily']['datapoints'].append({'timestamp': curr_time, 'value': dict(counters), 'idx': datapoint_idx})
            curr_time += Constants.SECONDS_IN_MIN
        print counters, '\n'

    def __aggregate__(self):

        for key in ['weekly', 'monthly']:
            key_tail = 0
            if len(self.__data__[key]['datapoints']) != 0:
                key_tail = self.__data__[key]['datapoints'][-1]['idx']
            
            daily_head = self.__data__['daily']['datapoints'][0]['idx']

            ## new events contains all daily events not captured in the weekly summary yet
            new_events = list(self.__data__['daily']['datapoints'][key_tail - daily_head:])

            frame_size = StatTable.STAT_AGGREGATION_SIZES[key]
            n_frames = len(new_events) / frame_size
            for frame_idx in range(n_frames):
                ## turn the strings into component tuples
                frame = new_events[frame_idx*frame_size: (frame_idx+1)*frame_size]

                ## last frame is incomplete; let's ignore it for now
                if len(frame) != frame_size: break

                ## extract the value for each metric
                counter = dict([(x, 0.0) for x in frame[0]['value'].keys()])

                ## it will keep the maximum value for each metric
                for datapoint in frame:
                    for metric in [LogLineEvent.USERS, LogLineEvent.AUDIO, LogLineEvent.VIDEO, LogLineEvent.ROOM]:
                        if counter[metric] < datapoint['value'][metric]:
                            counter[metric] = datapoint['value'][metric] 

                ## the timestamp for the value will be the one from the last daily event
                curr_time = float(frame[-1]['timestamp'])

                ## the index for the value will be the one from the last daily event, also
                datapoint_idx = int(frame[-1]['idx'])

                ## add to our datapoints
                self.__data__[key]['datapoints'].append({'timestamp': curr_time, 'value': counter, 'idx': datapoint_idx})

    def update(self, events):
        """
        Note: we assume events is sorted by timestamp
        """
        print events

        if len(events) > 0:
            if len(self.__data__['daily']['datapoints']) == 0:
                # no data yet, so we start scanning dates from
                # the start of the events list
                print "Initial logging"
                self.__append__(events)
            else:
                # if we already have some data in the file, we
                # start scanning from the last timestamp in the file
                latest = self.__data__['daily']['datapoints'][-1]
                print "Appending to log"
                self.__append__(events, latest)

        if len(self.__data__['daily']['datapoints']) > 0:
            self.__aggregate__()

        self.__slideWindow__()
        self.__writeFile__()
