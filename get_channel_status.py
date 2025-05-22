from MQPageReader import *
from MQueue import *
from Probe import *
import json, sys, web_util

if __name__ == "__main__":
  try:
    cookie = ""
    url = ""
    status = ""
    if len(sys.argv) > 3:
      cookie = sys.argv[1]
      url = sys.argv[2]
      status = sys.argv[3]
    elif len(sys.argv) > 2:
      cookie = sys.argv[1]
      url = sys.argv[2]
    else:
      raise IndexError
    probe = Probe(cookie, url)
    probe.loadUrl()
    thePage = probe.responseData["returnedText"].split("\n")
    if not isSSOPage(thePage):
      myQueuePage = MQPageReader(thePage, "CHANNELS")
      print json.dumps(makeHashOutOfQueueList(myQueuePage.getQueuesWithStaleMessages()), sort_keys = True, indent = 2)
    else:
      print json.dumps(makeHashOutOfQueueList([MQueue(probe.responseData["redirect-url"])]), sort_keys = True, indent = 2)
  except IndexError:
    print "usage: python get_channel_status.py [cookie] [url] [status]"
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
