import web
import json
        
urls = (
    '/stats/(.*)', 'MConfStatisticsWebService'
)
app = web.application(urls, globals())

path_prefix = './'
class MConfStatisticsWebService:
    URLToPathMap = {
        'daily': path_prefix+'daily.log',
        'weekly': path_prefix + 'weekly.log',
        'monthly': path_prefix + 'monthly.log'
    }

    def parseLog(self, filename):
        f = open(filename, 'r')
        lines = f.readlines()
        f.close()

        data = {}
        data['points'] = []
        for line in lines:
            tokens = line.split()
            data['points'].append([float(tokens[0]), float(tokens[1])])

        return data

    def GET(self, window):
        if not window:
            window = 'monthly'

        filename = MConfStatisticsWebService.URLToPathMap[window]

        data = self.parseLog(MConfStatisticsWebService.URLToPathMap[window])
        return json.dumps(data)

if __name__ == "__main__":
    app.run()
