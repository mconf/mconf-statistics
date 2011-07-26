class StatTable:
    """
    StatTable is the class that interfaces with the files containing the stats
    for important bigbluebutton events. The main method is update(events), which loads
    the current table from file (a text file or SQL) and adds the events that haven't been
    accounted for yet, doing proper aggregation.
    """

    STAT_TABLE_DAILY = 0
    STAT_TABLE_WEEKLY = 1
    STAT_TABLE_MONTHLY = 2

    def __init__(self, filename, _type):
        self.filename = filename
        self.__type__ = _type

    def __parseLine__(self, line):
        """
        line comes as [timestamp] [value]
        return: (timestamp: Float, value: Float)
        """
        return tuple([float(x) for x in line.split()])

    def type(self):
        return self.__type__

    def update(self, events):
        """
        Note: we assume events is sorted by timestamp
        """

        # we will possibly append new stats to the file, so open with 'a'
        # this file contains lines for each stat point, in the format
        # [timestamp] [value],
        # where [timestamp] is a float for seconds since the epoch (Unix timestamp)
        # and [value] is the data point
        f = file(self.filename, 'a')
        
        # gracefully handle an empty file
        tail = None
        try:
            tail = list(f.readlines())[-1]
            pass
        except IndexError:
            pass

        if tail is None:
            pass
        else:
            most_recent = self.__parseLine__(tail)
        

        f.close()

        
