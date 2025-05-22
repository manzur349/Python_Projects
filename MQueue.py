class MQueue(object):

  def __init__(self, q):
    self.queueName = q
    self.currentDepth = ""
    self.maxDepth = ""
    self.depth = ""
    self.lastPut = ""
    self.lastGet = ""
    self.msgAge = ""
    self.channelType = ""
    self.channelTRP = ""
    self.channelStatus = ""

  def setCurrentDepth(self, d):
    self.currentDepth = d

  def getCurrentDepth(self):
    return self.currentDepth

  def setMaxDepth(self, m):
    self.maxDepth = m

  def setMsgAge(self, a):
    self.msgAge = a

  def getMsgAge(self):
    return self.msgAge

  def setDepth(self, d):
    self.depth = d

  def getDepth(self):
    return self.depth

  def setLastPut(self, l):
    self.lastPut = l

  def getLastPut(self):
    return self.lastPut

  def setLastGet(self, l):
    self.lastGet = l

  def getLastGet(self):
    return self.lastGet

  def getQueueName(self):
    return self.queueName

  def setChannelType(self, c):
    self.channelType = c

  def setChannelTRP(self, c):
    self.channelTRP = c

  def setChannelStatus(self, c):
    self.channelStatus = c

  def getChannelType(self):
    return self.channelType

  def getChannelTRP(self):
    return self.channelTRP

  def getChannelStatus(self):
    return self.channelStatus
