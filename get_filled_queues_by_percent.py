from MQPageReader import *
from MQueue import *
from Probe import *
import json, sys
import web_util as wu

if __name__ == "__main__":
  try:
    cookie = ""
    url = ""
    percent = ""
    if len(sys.argv) > 3:
      cookies = sys.argv[1]
      url = sys.argv[2]
      percent = sys.argv[3]
    else:
      myFizzle = open("input_test.txt", "r")
      myLine = myFizzle.readline()
      while myLine:
        myLine = myLine.strip()
        if "EAGLE" in myLine.upper() and "PERCENT" in myLine.upper():
          myTokens = myLine.split("|")
          cookies = myToken[3]
          url = myTokens[1]
          percent = myTokens[2]
          break
        myLine = myFizzle.readline()
      myFizzle.close()
    if wu.isNumerical(percent):
      probe = Probe(cookie, url)
      probe.loadUrl()
      thePage = probe.responseData["returnedText"].split("\n")
      if not isSSOPage(thePage):
        myQueuePage = MQPageReader(thePage)
        print json.dumps(makeHashOutOfQueueList(myQueuePage.getFilledQueuesByPercentage(float(percent))), sort_keys = True, indent = 2)
      else:
        print json.dumps(makeHashOutOfQueueList([MQueue(probe.responseData["redirect-url"])]), sort_keys = True, indent = 2)
    else:
      raise IndexError
  except IndexError:
    print "usage: python get_filled_queues_by_percent.py [cookie] [url] [percentage]"
  except AttributeError as e:
    if "HTTPMESSAGE" in e.message.upper():
      print json.dumps(makeHashOutOfQueueList([MQueue("invalid_MQ_page_URL|" + probe.responseData["redirect-url"])]), sort_keys = True, indent = 2)
    else:
      if probe.responseData["http-return-code"] == 200:
        print json.dumps(makeHashOutOfQueueList([MQueue(probe.responseData["redirect-url"])]), sort_keys = True, indent = 2)
      else:
        print json.dumps(makeHashOutOfQueueList([MQueue("unresolved_error")]), sort_keys = True, indent = 2)
  except KeyError as e:
    if "RETURNEDTEXT" in e.message.upper():
      print json.dumps(makeHashOutOfQueueList([MQueue("invalid_MQ_page_URL|" + probe.responseData["redirect-url"])]), sort_keys = True, indent = 2)
    else:
      if probe.responseData["http-return-code"] == "200":
        print json.dumps(makeHashOutOfQueueList([MQueue(probe.responseData["redirect-url"])]), sort_keys = True, indent = 2)
      else:
        print json.dumps(makeHashOutOfQueueList([MQueue("unresolved_error")]), sort_keys = True, indent = 2)
