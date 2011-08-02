import time
import os

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

    def __init__(self, filenames, types):
        self.__filenames__ = filenames
        ## first thing we do is create the log files if they don't exist
        for filename in self.__filenames__.values():
            cmd = 'touch ' + filename
            os.system(cmd)

        self.__types__ = types
        self.__datapoints__ = {
            'daily': [],
            'weekly': [],
            'monthly': [],
            }

    def __parseLine__(self, line):
        """
        line comes as [timestamp] [value] [idx]
        return: (timestamp: Float, value: Float, idx:Int)
        """
        val = line.split()
        return {
            'timestamp': float(val[0]),
            'value': float(val[1]),
            'idx': int(val[2])
            }
    
    def types(self):
        return self.__types__

    def datapoints(self):
        return self.__datapoints__

    def __slideWindow__(self):
        """
        reads the files and deletes excessive lines from the start,
        to maintain the max allowable size for each file
        """
        for key in ['daily', 'weekly', 'monthly']:
            f = open(self.__filenames__[key], 'r')
            lines = list(f.readlines())
            f.close()
            if len(lines) > StatTable.STAT_TABLE_SIZES[key]:
                ## keep only the last self.size() elements
                lines = lines[len(lines) - StatTable.STAT_TABLE_SIZES[key]:]            

            f = open(self.__filenames__[key], 'w')
            f.writelines(lines)
            f.close()
            
    def __appendToFile__(self, f, events, curr_time, datapoint_idx):
        final_time = time.time()
        events_idx = 0
        counter = 0

        while curr_time < final_time:
            if events_idx < len(events):
                ## we're still within the existing events, so we check
                ## them to update the counter
                while events_idx < len(events) and events[events_idx].timestamp() < curr_time:
                    if events[events_idx].type() == self.types()[0]:
                        counter += 1
                    elif events[events_idx].type() == self.types()[1]:
                        counter -= 1
                    events_idx += 1
            
            self.__datapoints__['daily'].append((curr_time, counter, datapoint_idx))
            datapoint_idx += 1
            curr_time += Constants.SECONDS_IN_MIN

        ## now write the new data to the file
        lines = [str(x[0]) + ' ' + str(x[1]) + ' ' + str(x[2]) + '\n' for x in self.__datapoints__['daily']]
        f.writelines(lines)
        
    def __aggregate__(self):
        def head(f):
            result = int(list(f.readlines())[0].strip().split()[2])
            f.seek(0)
            return result

        def tail(f):
            result = int(list(f.readlines())[-1].strip().split()[2])
            f.seek(0)
            return result

        for key in ['weekly', 'monthly']:
            key_tail = 0
            if os.stat(self.__filenames__[key])[6] != 0:
                ## we already have some data for the weekly pass
                f = open(self.__filenames__[key])
                key_tail = tail(f)
                f.close()

            daily_file = open(self.__filenames__['daily'], 'r')
            daily_head = head(daily_file)

            ## new events contains all daily events not captured in the weekly summary yet
            new_events = list(daily_file.readlines())[key_tail - daily_head:]

            frame_size = StatTable.STAT_AGGREGATION_SIZES[key]
            n_frames = len(new_events) / frame_size
            for frame_idx in range(n_frames):
                ## turn the strings into component tuples
                frame = list(x.strip().split() for x in new_events[frame_idx*frame_size: (frame_idx+1)*frame_size])

                ## last frame is incomplete; let's ignore it for now
                if len(frame) != frame_size: break

                ## extract just the value
                val = sum(list(float(x[1]) for x in frame)) / len(frame)

                ## the timestamp for the value will be the one from the last daily event
                curr_time = float(frame[-1][0])

                ## the index for the value will be the one from the last daily event, also
                datapoint_idx = int(frame[-1][2])

                ## add to our datapoints
                self.__datapoints__[key].append((curr_time, val, datapoint_idx))

            ## now write the new data to the file
            lines = [str(x[0]) + ' ' + str(x[1]) + ' ' + str(x[2]) + '\n' for x in self.__datapoints__[key]]
            f = file(self.__filenames__[key], 'a+')
            f.writelines(lines)        
            f.close()

    def update(self, events):
        """
        Note: we assume events is sorted by timestamp
        """
        f = open(self.__filenames__['daily'], 'a+')
        if os.stat(self.__filenames__['daily'])[6] == 0:
            # if the file is empty, we start scanning the dates from
            # the start of the events list
            self.__appendToFile__(f, events, events[0].timestamp(), 0)
        else:
            # if we already have some data in the file, we
            # start scanning from the last timestamp in the file
            tail = list(f.readlines())[-1]
            latest = self.__parseLine__(tail)
            self.__appendToFile__(f, events, latest['timestamp'] + Constants.SECONDS_IN_MIN,
                                  latest['idx'] + 1)

        f.close()
        self.__aggregate__()
        self.__slideWindow__()
