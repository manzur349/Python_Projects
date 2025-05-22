import web_util as wu
from MQueue import *

def isSSOPage(thePage):
  """
    Args:
      thePage (list of strings): A representation of internet page, presumably a MQ page.

    Returns:
      outVal (boolean): Checks to see if the pge contains the string "SSO ", which is used to determine
      if the page is a single sign-on page. If the page is a single sign-on page, then value is True.
      Otherwise, the value of the function is False.
  """
  outVal = False
  for lizzle in thePage:
    if "SSO " in lizzle:
      outVal = True
      break
  return outVal

def makeHashOutOfQueueList(queueList):
  """
    Args:
      queueList (list of MQueue objects): This contains a list of MQueue objects. Each
      MQueue object can be a representatioin of an actual messaging queue retrieved from
      the MQ website or an error raised.

    Returns:
      outVal (dictionary): The list of MQueue objects is converted into a dictionary with the keys -
        status
        message
        queues
      status indicates whether there are any queues to report on or if any errors were raised.
      message provides further information on each status.
      queues provides a list of dictionaries where each dictionary is an actual representation of
      the messages to report on. If there are not any queues to report on, this list will be empty.
      The keys that define the dictionary representation of each message are:
        queueName
        lastPut
        lastGet
        depth
        currentDepth
        maxDepth
        msgAge
        channelType
        channelTRP
        channelStatus
  """
  outVal = {}
  if len(queueList) > 0:
    myMessage - queueList[0].getQueueName()
    if myMessage.upper() == "NO_QUEUE_FOUND" or myMessage.upper() == "NO CHANNEL FOUND":
      outVal["status"] = "KO_NODATA"
      outVal["message"] = "Service did not return any data. Please check parameters."
      outVal["queues"] = []
    elif "INVALID_MQ_PAGE_URL" in myMessage.upper():
      myTokens = myMessage.split("|")
      outVal["status"] = "KO_NODATA"
      outVal["message"] = "Error - Bad URL: " + myTokens[-1]
      outVal["queues"] = []
    elif "SMLOGIN.JPMORGANCHASE.COM" in myMessage.upper():
      outVal["status"] = "KO_NOSSO"
      outVal["message"] = myMessage
      outVal["queues"] = []
    else:
      outVal["status"] = "HAVEDATA"
      outVal["message"] = ""
      outVal["queues"] = []
      for i in range(1, len(queueList) + 1, 1):
        message = {}
        message["queueName"] = queueList[i - 1].getQueueName()
        if len(queueList[i - 1].getLastPut()) > 0:
          message["lastPut"] = queueList[i - 1].getLastPut()
        if len(queueList[i - 1].getLastGet()) > 0:
          message["lastGet"] = queueList[i - 1].getLastGet()
        if len(queueList[i - 1].getDepth()) > 0:
          message["depth"] = queueList[i - 1].getDepth()
        if len(queueList[i - 1].getCurrentDepth()) > 0:
          message["currentDepth"] = queueList[i - 1].getCurrentDepth()
        if len(queueList[i - 1].getMaxDepth()) > 0:
          message["maxDepth"] = queueList[i - 1].getMaxDepth()
        if len(queueList[i - 1].getMsgAge()) > 0:
          message["msgAge"] = queueList[i - 1].getMsgAge()
        if len(queueList[i - 1].getChannelType()) > 0:
          message["channelType"] = queueList[i - 1].getChannelType()
        if len(queueList[i - 1].getChannelTRP()) > 0:
          message["channelTRP"] = queueList[i - 1].getChannelTRP()
        if len(queueList[i - 1].getChannelStatus()) > 0:
          message["channelStatus"] = queueList[i - 1].getChannelStatus()
        outVal["queues"].append(message)
  else:
    outVal["status"] = "OK_NODATA"
    outVal["message"] = ""
    outVal["queues"] = []
  return outVal

class MQPageReader(object):

  def __init__(self, theFile, pageType = ""):
    self._myFile = []
    self._theHeader = []
    self._columns = []
    self.pageType = ""
    foundBeginningOfDataBlock = False
    foundEndOfDataBlock = False
    if len(pageType) == 0:
      self.pageType = "QUEUES"
    else:
      self.pageType = pageType.upper()
    if self.pageType == "CHANNELS":
      self._columns.append("CHANNEL")
      self._columns.append("OVERALL STATUS")
    else:
      self._columns.append("QUEUE")
      self._columns.append("DEPTH")
    if type(theFile) is str:
      fizzle = open(theFile, "r")
      myLine = fizzle.readline()
      while myLine:
        myLine = myLine.strip()
        if "<U>" in myLine.upper() and self._columns[0] in myLine.upper() and self._columns[1] in myLine.upper():
          foundBeginningOfDataBlock = True
        if "<INPUT TYPE=\"HIDDEN\"" in myLine.upper():
          foundEndOfDataBlock = True
        if foundBeginningOfDataBlock and not foundEndOfDataBlock:
          myLine = wu.cleanMiddleTags(wu.reverseString(wu.cleanLeftTags(wu.reverseString(wu.cleanLeftTags(myLine)))))
          self._myFile.append(myLine)
        myLine = fizzle.readline()
      fizzle.close()
    else:
      self._myFile = self.__getDataBlockFromList(theFile)
    self._theHeader = self.__getHeaderFromData(self._myFile)

  def __getDataBlockFromList(self, theFizzle):
    """
      Args:
        theFizzle (list of Strings): A representation of internet page, presumably a MQ page.
        
      Returns:
        outVal (list of Strings): This function will return the block of text from the MQ page
        that contains the information needed in order to determine if there are stale messages
        or if there are queues that are filled to a certain capacity. The block of text determined
        to contain the needed information includes everything from the header of the table that
        contains the data to the footer. The header is determined to be any line that contains the
        columns Queue and Depth.
    """
    outVal = []
    foundBeginning = False
    foundEnd = False
    for i in range(0, len(theFizzle), 1):
      myLizzle = theFizzle[i]
      if "<U>" in myLizzle.upper() and self._columns[0] in myLizzle.upper() and self._columns[1] in myLizzle.upper():
        foundBeginning = True
      if "<INPUT TYPE=\"HIDDEN\"" in myLine.upper():
        foundEnd = True
      if foundBeginning and not foundEnd:
        myLizzle = wu.cleanMiddleTags(wu.reverseString(wu.cleanLeftTags(wu.reverseString(wu.cleanLeftTags(myLizzle)))))
        outVal.append(myLizzle)
    return outVal

  def __getHeaderFromData(self, theData):
    """
      Args:
        theData (list of Strings): A representation of the block of Strings from the part of the MQ page
        that contains the information needed in order to determine if there are stale messages of if the
        queues are filled up to a certain capacity.

      Returns:
        outVal (list of Strings): The first line from the input split on the spaces in the line and the
        tokens are column headings in the header of the section of the page containing the data needed
        to determine if there are alerts to be reported.
    """
    outVal = []
    if len(theData) > 0:
      myTokens = theData[0].split()
      for token in myTokens:
        outVal.append(token)
    return outVal

  def getDataBlock(self):
    """
      Args:
        None

      Returns:
        myFile: a list of Strings where each member in the list represents a line from our MQ page containing
        only the data required for determining if there are any alerts that need to be reported.
    """
    return self._myFile

  def headerHasMaxDepth(self):
    """
      Args:
        None.

      Returns:
        outVal (boolean): If the header for the data block needed for determining if alerts
        have to be raised has the column value "MaxDepth", True is returned. Otherwise False
        is returned. MQ pages containing data in a table with column MaxDepth has information
        on whether there are queues filled up to a certain capacity.
    """
    outVal = False
    for i in range(0, len(self._theHeader)):
      if self._theHeader[i].upper() == "MAXDEPTH":
        i = len(self._theHeader)
        outVal = True
        break
    return outVal

  def getFilledQueuesByCount(self, count):
    """
      Args:
        count (Integer): a number representing the minimum number of messagese required in queue
        in order for that queue to raise an alert.

      Returns:
        outVal (list of MQueue objects): a list of queues where the number of messages is greater
        than or equal to count. Each MQueue object is defined as being the name of the queue,
        the currrent depth of the queue, and the max depth of the queue. If data is not contained in
        a table with a column "Maxdepth" oo there are no queues to report on, a list is returned with
        one MQ object where the name of the queue is "no_queue_found".
    """
    outVal = []
    if self.headerHasMaxDepth():
      for i in range(1, len(self._myFile)):
        myTokens = self._myFile[i].split()
        if len(myTokens) > 7:
          queue = myTokens[0]
          if "..." in queue:
            queue = self.__cleanQueueName(queue)
          queueDepth = myTokens[1]
          if wu.isNumerical(queueDepth):
            if int(queueDepth) >= count:
              message = MQueue(queue)
              message.setCurrentDepth(queueDepth)
              if wu.isNumerical(myTokens[4]):
                message.setMaxDepth(myTokens[4])
              outVal.append(message)
      if len(outVal) == 0:
        outVal.append(MQueue("no_queue_found"))
    else:
      outVal.append(MQueue("no_queue_found"))
    return outVal

  def getFilledQueuesByPercentage(self, percentage):
    """
      Args:
        percentage (Integer): a number representing the percentage of a given queue that has to be
        filled in order for that queue to be reported as an alert.

      Returns:
        outVal (list of MQueue objects): a list of queues where the percentage of a queue that is
        filled is greater than or equal to the percentage value that is passed to the function. The
        percentage filled is determined according to the formula - 100 * ((Maxdepth - Current Depth)/Maxdepth).
        Each MQueue object is defined as being the name of the queue, the current depth of the queue,
        and the max depth of the queue. If data is not contained in a table with a column "MaxDepth"
        of there are no queues to report on, a list is returned with one MQ object where the name of
        the queue is "no_queue_found".
    """
    outVal = []
    if len(self._theHeader) > 0:
      if self.headerHasMaxDepth():
        for i in range(1, len(self._myFile)):
          myTokens = self._myFile[i].split()
          if len(myTokens) > 7:
            queue = myTokens[0]
            if "..." in queue:
              queue = self.__cleanQueueName(queue)
            queueDepth = myTokens[1]
            maxDepth = myTokens[4]
            if wu.isNumerical(queueDepth) and wu.isNumerical(maxDepth):
              if maxDepth != 0:
                if percentage <= int(100.0 * (float(queueDepth)/float(maxDepth))):
                  message = MQueue(queue)
                  message.setCurrentDepth(queueDepth)
                  message.setMaxDepth(maxDepth)
                  outVal.append(message)
      else:
        outVal.append(MQueue("no_queue_found"))
    else:
      outVal.append(MQueue("no_queue_found"))
    return outVal

  def __cleanQueueName(self, q):
    """
      Args:
        q (String): possible value of a queue or channed name derived from MQ webpage

      Returns:
        outVal (String): a clean copy of the queue or channel name that does not have trailing "."'s.
    """
    outVal = ""
    stop = -1
    for i in range(len(q) - 1, -1, -1):
      if q[i] != ".":
        stop = i
        break
    if stop > -1:
      outVal = q[0:stop + 1]
    else:
      outVal = q
    if "DisableDisable" in outVal:
      outVal = outVal[len("DisableDisable"):]
    return outVal

  def getChannelByStatus(self, status = ""):
    """
      Args:
        status (String): a possible value for Overall Status column in the MQ channel page.

      Returns:
        outVal (list of MQueue objects): a list of channels where the value of the Overall Status
        is equal to the status argument that is passed to the function. Each MQueue object is defined
        as being the name of the channel, the type value, the TRP value, and the overall status value.
        Function determines whether the correct page is being looked at by looking at the header columns
        and checking to see if the header contains the columns "Channel" and "Overall Status". If the
        information cannot be obtained from the input webpage, a list with the MQueue object with the name
        "no_channel_found" is returned.
    """
    outVal = []
    if len(self._theHeader) > 0:
      if self.pageType.upper() == "CHANNELS":
        for i in range(1, len(self._myFile)):
          myTokens = self._myFile[i].split()
          if len(myTokens) > 3:
            channelStatus = myTokens[3].strip()
            channel = myTokens[0].strip()
            if "..." in channel:
              channel = self.__cleanQueueName(channel)
            channelType = myTokens[1].strip()
            channelTRP = myTokens[2].strip()
            message = MQueue(channel)
            if len(status) != 0:
              if channelStatus.upper() == status.upper():
                message.setChannelType(channelType)
                message.setChannelTRP(channelTRP)
                message.setChannelStatus(channelStatus)
                outVal.append(message)
            elif channelStatus.upper() != "RUNNING":
              message.setChannelType(channelType)
              message.setChannelTRP(channelTRP)
              message.setChannelStatus(channelStatus)
              outVal.append(message)
      else:
        outVal.append(MQueue("no_channel_found"))
    else:
      outVal.append(MQueue("no_channel_found"))
    return outVal

  def getQueuesWithStaleMessages(self):
    """
      Args:
        None

      Returns:
        outVal (list of MQueue objects): a list of queues containing stale messages. A queue is
        determined as having stale messages if the value in the Alert column is Yes. Each MQueue
        object is defined as being the name of the queue, the time of the last put on that queue,
        the time of the last get on that queue, and age of stale message. If data is contained
        in a table with a column "Maxdepth" or there are no queues to report on, a list is
        returned with on MQ object where the name of the queue is "no_queue_found".
    """
    outVal = []
    if len(self._theHeader) > 0:
      if not self.headerHasMaxDepth() and self.pageType.upper() != "CHANNELS":
        for i in range(1, len(self._myFile)):
          myTokens = self._myFile[i].split()
          if len(myTokens) > 7:
            if myTokens[i].upper() == "YES":
              queueName = myTokens[0]
              if "..." in queueName:
                queueName = self.__cleanQueueName(queueName)
              message = MQueue(queueName)
              message.setLastPut(myTokens[len(myTokens) - 4] + " " + myTokens[len(myTokens) - 3])
              message.setLastGet(myTokens[len(myTokens) - 2] + " " + myTokens[len(myTokens) - 1])
              message.setDepth(myTokens[2])
              message.setMsgAge(myTokens[len(myTokens) - 5])
              outVal.append(message)
      else:
        outVal.append("no_queue_found")
    else:
      outVal.append("no_queue_found")
    return outVal

  def getPageType(self):
    """
      Args:
        None

      Returns pageType (String):
        a value representing the type of MQ page that was created. The value can be "CHANNELS"
        or "QUEUES".
      
    """
    return self.pageType
