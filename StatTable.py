import time
import os
import json

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
            
    def __append__(self, events, latest_datapoint=None):
        curr_time = events[0].timestamp()
        datapoint_idx = 0

        counters = {
            LogLineEvent.LOG_LINE_EVENT_USERS: 0,
            LogLineEvent.LOG_LINE_EVENT_AUDIO: 0,
            LogLineEvent.LOG_LINE_EVENT_VIDEO: 0,
            LogLineEvent.LOG_LINE_EVENT_ROOM: 0,
            'users': {},
            'usernames': {},
            'audio_ids': {},
        }

        if latest_datapoint:
            curr_time = latest_datapoint['timestamp'] + Constants.SECONDS_IN_MIN
            datapoint_idx = latest_datapoint['idx'] + 1
            counters = {
                LogLineEvent.LOG_LINE_EVENT_USERS: latest_datapoint['value'][LogLineEvent.LOG_LINE_EVENT_USERS],
                LogLineEvent.LOG_LINE_EVENT_AUDIO: latest_datapoint['value'][LogLineEvent.LOG_LINE_EVENT_AUDIO],
                LogLineEvent.LOG_LINE_EVENT_VIDEO: latest_datapoint['value'][LogLineEvent.LOG_LINE_EVENT_VIDEO],
                LogLineEvent.LOG_LINE_EVENT_ROOM: latest_datapoint['value'][LogLineEvent.LOG_LINE_EVENT_ROOM],
                'users': dict(latest_datapoint['value']['users']),
                'usernames': dict(latest_datapoint['value']['usernames']),
                'audio_ids': dict(latest_datapoint['value']['audio_ids']),
            }

        final_time = time.time()
        events_idx = 0

        increments = {
            LogLineEvent.LOG_LINE_EVENT_USER_JOIN: 1,
            LogLineEvent.LOG_LINE_EVENT_USER_LEAVE: -1,
            
            LogLineEvent.LOG_LINE_EVENT_AUDIO_START: 1,
            LogLineEvent.LOG_LINE_EVENT_AUDIO_STOP: -1,

            LogLineEvent.LOG_LINE_EVENT_VIDEO_START: 1,
            LogLineEvent.LOG_LINE_EVENT_VIDEO_STOP: -1,

            LogLineEvent.LOG_LINE_EVENT_ROOM_CREATE: 1,
            LogLineEvent.LOG_LINE_EVENT_ROOM_DESTROY: -1
        }

        while curr_time < final_time:
            if events_idx < len(events):
                ## we're still within the existing events, so we check
                ## them to update the counter
                while events_idx < len(events) and events[events_idx].timestamp() < curr_time:
                    curr_event = events[events_idx]
                    event_type = LogLineEvent.EventTypeMap[curr_event.type()]

                    ## specific event handling
                    if curr_event.type() == LogLineEvent.LOG_LINE_EVENT_USER_JOIN:
                        ## the user is joining, so we add him/her to the persisent list of users
                        counters['users'][curr_event.id()] = { 'audio': False, 'video': False }
                    
                    elif curr_event.type() == LogLineEvent.LOG_LINE_EVENT_USER_NAME:
                        ## the user is being named, we must track this name for the audio start/stop events
                        counters['usernames'][curr_event.username()] = curr_event.id()

                    elif curr_event.type() == LogLineEvent.LOG_LINE_EVENT_USER_LEAVE:
                        ## the user is leaving, so we must decrement the audio/video counters
                        ## if the audio/video streams are true, and delete the entry
                        ## from the users dictionary in the datapoints
                        if counters['users'][curr_event.id()]['audio']:
                            counters[LogLineEvent.LOG_LINE_EVENT_AUDIO] -= 1
                        if counters['users'][curr_event.id()]['video']:
                            counters[LogLineEvent.LOG_LINE_EVENT_VIDEO] -= 1
                        del counters['users'][curr_event.id()]

                    ## start/stop video
                    elif curr_event.type() == LogLineEvent.LOG_LINE_EVENT_VIDEO_START:
                        try: counters['users'][curr_event.id()]['video'] = True
                        except KeyError: pass
                    elif curr_event.type() == LogLineEvent.LOG_LINE_EVENT_VIDEO_STOP:
                        try: counters['users'][curr_event.id()]['video'] = False
                        except KeyError: pass

                    ## start/stop audio
                    elif curr_event.type() == LogLineEvent.LOG_LINE_EVENT_AUDIO_ID:
                        ## we acquire the audio id for the user
                        counters['audio_ids'][curr_event.id()] = counters['usernames'][curr_event.username()]

                    elif curr_event.type() == LogLineEvent.LOG_LINE_EVENT_AUDIO_START:
                        user_id = counters['usernames'][curr_event.username()]
                        try: counters['users'][curr_event.id()]['audio'] = True
                        except KeyError: pass

                    elif curr_event.type() == LogLineEvent.LOG_LINE_EVENT_AUDIO_STOP:
                        user_id = counters['audio_ids'][curr_event.id()]
                        try: counters['users'][curr_event.id()]['audio'] = False
                        except KeyError: pass                        

                    try: counters[event_type] += increments[events[events_idx].type()]
                    except KeyError: pass

                    events_idx += 1

            self.__data__['daily']['datapoints'].append({'timestamp': curr_time, 'value': dict(counters), 'idx': datapoint_idx})

            datapoint_idx += 1
            curr_time += Constants.SECONDS_IN_MIN

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

                for datapoint in frame:
                    for metric in datapoint['value'].keys():
                        counter[metric] += datapoint['value'][metric]
                for metric in counter.keys():
                    counter[metric] /= float(len(frame))

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

        if len(self.__data__['daily']['datapoints']) == 0:
            # no data yet, so we start scanning dates from
            # the start of the events list
            self.__append__(events)
        else:
            # if we already have some data in the file, we
            # start scanning from the last timestamp in the file
            latest = self.__data__['daily']['datapoints'][-1]
            self.__append__(events, latest)

        self.__aggregate__()
        self.__slideWindow__()
        self.__writeFile__()
