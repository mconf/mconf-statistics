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

    def __init__(self, filenames):
        self.__filenames__ = filenames
        ## first thing we do is create the log files if they don't exist
        for filename in self.__filenames__.values():
            cmd = 'touch ' + filename
            os.system(cmd)

        self.__datapoints__ = {
            'daily': [],
            'weekly': [],
            'monthly': [],
            }

    def __writeFile__(self, filename, datapoints):
        # writes the data as a JSON-encoded dict
        f = open(filename, 'w')

        # this is the fundamental JSON object that is written to the file
        json_obj = {
            'datapoints': datapoints
        }

        filestring = json.dumps(json_obj) + '\n'
        f.write(filestring)
        f.close()

    def __readFile__(self, filename):
        # reads a JSON-encoded object from file
        f = open(filename, 'r')

        # default object for empty files
        obj = {'datapoints': []}

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
            data = self.__readFile__(self.__filenames__[key])
            if len(data['datapoints']) > StatTable.STAT_TABLE_SIZES[key]:
                data['datapoints'] = data['datapoints'][len(data['datapoints']) - StatTable.STAT_TABLE_SIZES[key]:]
            self.__writeFile__(self.__filenames__[key], data['datapoints'])
            
    def __appendToFile__(self, filename, events, curr_time, datapoint_idx):
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

            self.__datapoints__['daily'].append({'timestamp': curr_time, 'value': counters, 'idx': datapoint_idx})

            datapoint_idx += 1
            curr_time += Constants.SECONDS_IN_MIN

        ## now write the new data to the file
        self.__writeFile__(filename, self.__datapoints__['daily'])

    def __aggregate__(self):
        for key in ['weekly', 'monthly']:
            key_tail = 0

            data = self.__readFile__(self.__filenames__[key])

            if len(data['datapoints']) != 0:
                key_tail = data['datapoints'][-1]['idx']

            daily_data = self.__readFile__(self.__filenames__['daily'])
            daily_head = daily_data['datapoints'][0]['idx']

            ## new events contains all daily events not captured in the weekly summary yet
            new_events = list(daily_data['datapoints'][key_tail - daily_head:])

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
                self.__datapoints__[key].append({'timestamp': curr_time, 'value': counter, 'idx': datapoint_idx})

            ## now write the new data to the file
            self.__writeFile__(self.__filenames__[key], self.__datapoints__[key])

    def update(self, events):
        """
        Note: we assume events is sorted by timestamp
        """
        ### the file can be empty, but must exist
        data = self.__readFile__(self.__filenames__['daily'])

        if len(data['datapoints']) == 0:
            # no data yet, so we start scanning dates from
            # the start of the events list
            self.__appendToFile__(self.__filenames__['daily'], events, events[0].timestamp(), 0)
        else:
            # if we already have some data in the file, we
            # start scanning from the last timestamp in the file

            # first, we read all the existing data to self.__datapoints__
            self.__datapoints__['daily'] = data['datapoints']

            latest = data['datapoints'][-1]
            self.__appendToFile__(self.__filenames__['daily'], events, latest['timestamp'] + Constants.SECONDS_IN_MIN,
                                  latest['idx'] + 1)

        self.__aggregate__()
        self.__slideWindow__()
