import socorro.lib.ver_tools as vtl


#=================================================================================================================
class LegacyThrottler(object):
  #-----------------------------------------------------------------------------------------------------------------
  def __init__(self, config):
    self.config = config
    self.processedThrottleConditions = self.preprocessThrottleConditions(config.throttleConditions)
  #-----------------------------------------------------------------------------------------------------------------
  ACCEPT = 0
  DEFER = 1
  DISCARD = 2
  #-----------------------------------------------------------------------------------------------------------------
  @staticmethod
  def regexpHandlerFactory(regexp):
    def egexpHandler(x):
      return regexp.search(x)
    return egexpHandler

  #-----------------------------------------------------------------------------------------------------------------
  @staticmethod
  def boolHandlerFactory(aBool):
    def boolHandler(dummy):
      return aBool
    return boolHandler

  #-----------------------------------------------------------------------------------------------------------------
  @staticmethod
  def genericHandlerFactory(anObject):
    def genericHandler(x):
      return anObject == x
    return genericHandler

  #-----------------------------------------------------------------------------------------------------------------
  def preprocessThrottleConditions(self, originalThrottleConditions):
    newThrottleConditions = []
    for key, condition, percentage in originalThrottleConditions:
      #print "preprocessing %s %s %d" % (key, condition, percentage)
      conditionType = type(condition)
      if conditionType == compiledRegularExpressionType:
        #print "reg exp"
        newCondition = LegacyThrottler.regexpHandlerFactory(condition)
        #print newCondition
      elif conditionType == bool:
        #print "bool"
        newCondition = LegacyThrottler.boolHandlerFactory(condition)
        #print newCondition
      elif conditionType == functionType:
        newCondition = condition
      else:
        newCondition = LegacyThrottler.genericHandlerFactory(condition)
      newThrottleConditions.append((key, newCondition, percentage))
    return newThrottleConditions

  #-----------------------------------------------------------------------------------------------------------------
  def understandsRefusal (self, jsonData):
    try:
      return vtl.normalize(jsonData['Version']) >= vtl.normalize(self.config.minimalVersionForUnderstandingRefusal[jsonData['ProductName']])
    except KeyError:
      return False

  #-----------------------------------------------------------------------------------------------------------------
  def applyThrottleConditions (self, jsonData):
    """cycle through the throttle conditions until one matches or we fall off
    the end of the list.
    returns:
      True - reject
      False - accept
    """
    #print processedThrottleConditions
    for key, condition, percentage in self.processedThrottleConditions:
      #logger.debug("throttle testing  %s %s %d", key, condition, percentage)
      throttleMatch = False
      try:
        throttleMatch = condition(jsonData[key])
      except KeyError:
        if key == None:
          throttleMatch = condition(None)
        else:
          #this key is not present in the jsonData - skip
          continue
      except IndexError:
        pass
      if throttleMatch: #we've got a condition match - apply the throttle percentage
        randomRealPercent = random.random() * 100.0
        #logger.debug("throttle: %f %f %s", randomRealPercent, percentage, randomRealPercent > percentage)
        return randomRealPercent > percentage
    # nothing matched, reject
    return True

  #-----------------------------------------------------------------------------------------------------------------
  def throttle (self, jsonData):
    if self.applyThrottleConditions(jsonData):
      #logger.debug('yes, throttle this one')
      if self.understandsRefusal(jsonData) and not self.config.neverDiscard:
        logger.debug("discarding %s %s", jsonData.ProductName, jsonData.Version)
        return LegacyThrottler.DISCARD
      else:
        logger.debug("deferring %s %s", jsonData.ProductName, jsonData.Version)
        return LegacyThrottler.DEFER
    else:
      logger.debug("not throttled %s %s", jsonData.ProductName, jsonData.Version)
      return LegacyThrottler.ACCEPT

