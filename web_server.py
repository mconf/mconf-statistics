import web
import json
import sys
        
urls = (
    '/stats/(.*)', 'MConfStatisticsWebService'
)
app = web.application(urls, globals())

class MConfStatisticsWebService:
    def GET(self, window):
        if not window:
            window = 'monthly'

        obj = json.loads(file(sys.argv[2]).read())[window]

        # we take the first datapoint as a template for all others
        output = {}
        keys = obj['datapoints'][0]['value'].keys()
        for key in keys:
            output[key] = []
            for datapoint in obj['datapoints']:
                output[key].append([datapoint['timestamp'], datapoint['value'][key]])

        return json.dumps(output)

if __name__ == "__main__":
    app.run()
