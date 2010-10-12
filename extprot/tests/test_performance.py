
import sys
import cPickle
import timeit
from extprot import types

class CP_Message(object):
    def __init__(self,label,count):
        self.label = label
        self.count = count

class EP_Message(types.Message):
    label = types.Field(types.String)
    count = types.Field(types.Int)

CP_Msg1 = CP_Message("hello world",42)
EP_Msg1 = EP_Message("hello world",42)




if __name__ == "__main__":
    cptmr = timeit.Timer("cPickle.loads(cPickle.dumps(CP_Msg1))","from extprot.tests.test_performance import cPickle, CP_Msg1")
    cptimes = cptmr.repeat(3,10000)
    print "cPickle:", min(cptimes)
  
    eptmr = timeit.Timer("EP_Message.from_string(EP_Msg1.to_string())","from extprot.tests.test_performance import EP_Msg1, EP_Message")
    eptimes = eptmr.repeat(3,10000)
    print "extprot:", min(eptimes)
   

