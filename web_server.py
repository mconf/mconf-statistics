#! /usr/bin/python

import web
import json
import sys
import os
from daemon import Daemon

class MconfStatisticsWebService:
    def GET(self, window):
        i = web.input(window='all', callback='(function(obj){})')
        window = i.window
        callback = i.callback
        web.header('Content-Type', 'application/x-javascript')

        output = {}

        if window == 'all':
            output = json.loads(file(sys.argv[2]).read())
            
        else:
            obj = json.loads(file(sys.argv[2]).read())[window]

            # we take the first datapoint as a template for all others
            keys = obj['datapoints'][0]['value'].keys()
            for key in keys:
                output[key] = []
                for datapoint in obj['datapoints']:
                    output[key].append([datapoint['timestamp'], datapoint['value'][key]])

        # this is kinda bad, but it's what we need to implement
        # jsonp, otherwise it wouldn't work
        return callback + '(' + json.dumps(output) + ')'

class WSDaemon(Daemon):
    def run(self):
        urls = (
            '/stats/(.*)', 'MconfStatisticsWebService'
        )
        app = web.application(urls, globals())
        app.run();

if __name__ == "__main__":
    daemon = WSDaemon('/tmp/statistics-server.pid')
    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1] and len(sys.argv) >= 4: 
            del sys.argv[1:2]
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1] and len(sys.argv) >= 4:
            del sys.argv[1:2]
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
