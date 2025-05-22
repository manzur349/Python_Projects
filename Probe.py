import urllib2
import json
import datetime
import sys

class Probe():
  def __init__(self, cookie,url):
    self.type = "endpoint"
    self.url = url
    self.header = {}
    self.header["Accept"] = "text/plain, text/html, application/json"
    self.header["Cookie"] = "SMSESSION=" + cookie
    self.node = None
    self.tests = []
    self.responseData = {}
    self.probeErrors = []
    self.lastResult = {}
    self.lastStatus = {}

  def loadUrl(self):
    try:
      req = urllibs2.Request(self.url, None, self.header)
      handle = urllib2.urlopen(req, None, 7)

      self.responseData["http-return-code"] = handle.getcode()
      self.responseData["redirect-url"] = handle.geturl()
      headersDict = handle.headers.dict
      self.responseData["headers"] = headersDict
      if "content-type" in headersDict.iterkeys():
        contentType = headersDict["content-type"]
        self.responseData["return-content"] = contentType
        if "application/json" in str(contentType):
          self.responseData["json"] = json.loads(handle.read())
        elif "text/plain" in str(contentType) or "application/javascript" in str(contentType) or "text/html" in str(contentType):
          self.responseData["returnedText"] = handle.read()
        else:
          str(handle.getcode()) + " content-type:" + str(headersDict["content-type"]) + " on url: " + handle.geturl()
      else:
        print "No content-type returned" + headersDict

      self.lastResult["timestamp"] = datetime.datetime.utcnow()
      handle.close()
    except urllib2.HTTPError, err:
      self.lastResult["exception"] = err.info()
      self.responseData["redirect-url"] = self.url
      self.responseData["http-return-code"] = err.code
    except urllib2.URLError, err:
      self.responseData["redirect-url"] = self.url
      self.responseData["http-return-code"] = err.errno
      self.lastResult["exception"] = err.message
