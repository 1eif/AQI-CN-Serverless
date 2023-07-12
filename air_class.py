#-*- encoding: utf-8 -*-
import requests, re, base64, zlib, xmltodict, json
from io import BytesIO , StringIO
from wcf.records import *
from wcf.xml2records import XMLParser

# https://github.com/ernw/python-wcfbin

class airChina():
    def __init__(self):
        pass

    def getResponse(self, action, data):
        output = StringIO()
        output.write('<'+action+' xmlns="http://tempuri.org/">'+data+'</'+action+'>')
        output.seek(0)

        r = XMLParser.parse(output)
        req = dump_records(r)

        r = requests.post(url='https://air.cnemc.cn:18007/ClientBin/Env-CnemcPublish-RiaServices-EnvCnemcPublishDomainService.svc/binary/'+action,
            data=req,
            headers={'Content-Type': 'application/msbin1', 'Referer': 'https://air.cnemc.cn:18007/ClientBin/cnemc.xap'}, verify=False)
        res = r.content

        buf = BytesIO(res)
        r = Record.parse(buf)

        print_records(r, fp=output)
        output.seek(0)

        pat = re.compile('<[^>]+>')
        enc = pat.sub('', output.readlines()[1][1:])[:-1]

        enc = base64.b64decode(enc)
        enc = zlib.decompress(enc)

        convertedDict = xmltodict.parse(enc)
        return json.dumps(convertedDict)

    def getAllStationsData(self):
        return json.loads(self.getResponse("GetAllAQIPublishLive", ""))["ArrayOfAQIDataPublishLive"]["AQIDataPublishLive"]