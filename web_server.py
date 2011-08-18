import web
import json
import sys
        
urls = (
    '/stats/(.*)', 'MConfStatisticsWebService'
)
app = web.application(urls, globals())

class MConfStatisticsWebService:
    def GET(self, window):
        i = web.input(window='monthly', callback='(function(obj){})')
        window = i.window
        callback = i.callback
        web.header('Content-Type', 'application/x-javascript')

        obj = json.loads(file(sys.argv[2]).read())[window]

        # we take the first datapoint as a template for all others
        output = {}
        keys = obj['datapoints'][0]['value'].keys()
        for key in keys:
            output[key] = []
            for datapoint in obj['datapoints']:
                output[key].append([datapoint['timestamp'], datapoint['value'][key]])

        # this is kinda bad, but it's what we need to implement
        # jsonp, otherwise it wouldn't work
        return callback + '(' + json.dumps(output) + ')'

if __name__ == "__main__":
    app.run()
