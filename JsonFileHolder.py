import json
import os


class JsonHolder:
    def __init__(self, filename):
        self.Filename = filename
        if os.path.exists(filename):
            if os.path.isfile(filename):
                try:
                    file = open(filename, 'r')
                    jsonstring = file.read()
                    if jsonstring == "":
                        self.jsondata = {}
                    else:
                        self.jsondata = json.loads(jsonstring)
                finally:
                    file.close()

        else:
            self.jsondata = {}
            try:
                file = open(self.Filename, 'w')
                file.write(json.dumps(self.jsondata, sort_keys=True, indent=4))
            finally:
                file.close()

    def getjsondata(self, key):
        return self.jsondata[key]

    def writejsondata(self, key, value):
        self.jsondata[str(key)] = value
        try:
            file = open(self.Filename, 'w')
            file.write(json.dumps(self.jsondata, sort_keys=True, indent=4))
        finally:
            file.close()
        pass
