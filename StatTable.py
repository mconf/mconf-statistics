import time
import os
import json

import Constants
from LogLineEvent import LogLineEvent

class StatTable:
    """
    StatTable is the class that interfaces with the files containing the stats
    for important bigbluebutton events. The main method is update(events), which loads
    the current table from file (a text file or SQL) and adds the events that haven't been
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
            
    def __append__(self, events, curr_time, datapoint_idx):
        final_time = time.time()
        events_idx = 0

        counters = {
            LogLineEvent.LOG_LINE_EVENT_USERS: 0,
            LogLineEvent.LOG_LINE_EVENT_AUDIO: 0,
            LogLineEvent.LOG_LINE_EVENT_VIDEO: 0,
            LogLineEvent.LOG_LINE_EVENT_ROOM: 0
        }

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
                    event_type = LogLineEvent.EventTypeMap[events[events_idx].type()]
                    counters[event_type] += increments[events[events_idx].type()]
                    events_idx += 1

            self.__data__['daily']['datapoints'].append({'timestamp': curr_time, 'value': counters, 'idx': datapoint_idx})

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
            self.__append__(events, events[0].timestamp(), 0)
        else:
            # if we already have some data in the file, we
            # start scanning from the last timestamp in the file
            latest = self.__data__['daily']['datapoints'][-1]
            self.__append__(events, latest['timestamp'] + Constants.SECONDS_IN_MIN, latest['idx'] + 1)

        self.__aggregate__()
        self.__slideWindow__()
        self.__writeFile__();
