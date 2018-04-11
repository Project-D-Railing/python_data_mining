import json
import os


class JsonHolder:
    def __init__(self, Filename):
        self.Filename = Filename
        if os.path.exists(Filename):
            if os.path.isfile(Filename):
                try:
                    file = open(Filename, 'r')
                    jsonstring = file.read()
                    if jsonstring == "":
                        self.jsondata = {}
                    else:
                        self.jsondata = json.loads(jsonstring)
                finally:
                    file.close()

    def getjsondata(self, key):
        return self.jsondata[key]

    def writejsondata(self, key, value):
        self.jsondata[key] = value
        if os.path.exists(self.Filename):
            if os.path.isfile(self.Filename):
                try:
                    file = open(self.Filename, 'w')
                    file.write(json.dumps(self.jsondata, sort_keys=True, indent=4))
                finally:
                    file.close()
        pass

