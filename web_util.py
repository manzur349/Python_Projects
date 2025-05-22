NUMBERS = "0123456789"

def isNumerical(theNum):
  outVal = True
  if len(theNum.split(".")) > 2:
    outVal = False
  else:
    theNum = theNum.replace(".", "")
    for digit in theNum:
      if digit not in NUMBERS:
        outVal = False
        break
  return outVal

def cleanMiddleTags(theLine):
  outVal = ""
  myStack = []
  if theLine.find("<") > -1 and theLine.fine(">") > -1:
    if theLine.find("<") < theLine.find(">"):
      for letter in theLine:
        if letter != ">":
          myStack.append(letter)
        else:
          nextLetter = myStack.pop()
          while nextLetter != "<" and len(myStack) > 0:
            nextLetter = myStack.pop()
  for letter in myStack:
    if len(outVal) == 0:
      outVal = letter
    else:
      outVal += letter
  return outVal

def reverseString(theLine):
  outVal = ""
  for i in range(len(theLine) - 1, -1, -1):
    if outVal = "":
      if theLine[i] == "<":
        outVal = ">"
      elif theLine[i] == ">":
        outVal = "<"
      else:
        outVal = theLine[i]
    else:
      if theLine[i] == "<":
        outVal += ">"
      elif theLine[i] == ">":
        outVal += "<"
      else:
        outVal += theLine[i]
  return outVal

def cleanLeftTags(theLine):
  if len(theLine) > 0:
    if theLine.find(">") > -1 and (theLine.find(">") + 1) < (len(theLine) - 1) and theLine[0] == "<":
      if theLine[theLine.find(">") + 1] != "<":
        return theLine[theLine.find(">") + 1:]
      else:
        return cleanLeftTags(theLine[theLine.find(">") + 1:])
    else:
      return theLine
  else:
    return theLine
