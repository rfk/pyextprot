
import sys
import cPickle
import timeit
import unittest
from extprot import types

from nose import SkipTest

#  Some simple classes to be serialized.

class EP_Person(types.Message):
    name = types.Field(types.String)
    telephone = types.Field(types.Int)

class EP_Message(types.Message):
    subject = types.Field(types.String)
    body = types.Field(types.String)
    sender = types.Field(EP_Person)
    recipients = types.Field(types.List.build(EP_Person))


#  And some equivalent pure-python classes for comparison.

class CP_Person(object):
    def __init__(self,name,telephone):
        self.name = name
        self.telephone = telephone

class CP_Message(object):
    def __init__(self,subject,body,sender,recipients=[]):
        self.subject = subject
        self.body = body
        self.sender = sender
        self.recipients = recipients


class TestPerformanceAgainstCPickle(unittest.TestCase):

    def _timeit(self,statement,*setup):
        setup_code = ["from extprot.tests.test_performance import CP_Person, CP_Message, EP_Person, EP_Message, cPickle"]
        setup_code.extend(setup)
        timer = timeit.Timer(statement,"\n".join(setup_code))
        return min(timer.repeat(3,10000))

    def assertFasterThan(self,cpt,ept):
        if types.serialize.__file__.endswith(".py"):
            raise SkipTest
        if types.serialize.__file__.endswith(".pyc"):
            raise SkipTest
        if cpt <= ept:
            assert False, "%s <= %s" % (cpt,ept)

    def test_simple_person_string_write(self):
        cpt = self._timeit("cPickle.dumps(p,-1)",
                           "p = CP_Person('Ryan Kelly',12345678)")
        ept = self._timeit("p.to_string()",
                           "p = EP_Person('Ryan Kelly',12345678)")
        self.assertFasterThan(cpt,ept)

    def test_simple_person_string(self):
        cpt = self._timeit("cPickle.loads(cPickle.dumps(p,-1))",
                           "p = CP_Person('Ryan Kelly',12345678)")
        ept = self._timeit("EP_Person.from_string(p.to_string())",
                           "p = EP_Person('Ryan Kelly',12345678)")
        self.assertFasterThan(cpt,ept)

    def test_single_recipient_message_string_write(self):
        cpt = self._timeit("cPickle.dumps(m,-1)",
                           "s = CP_Person('Ryan Kelly',12345678)",
                           "r = CP_Person('Lauren Kelly',98765)",
                           "m = CP_Message('hey there','hi!!!',s,[r])")
        ept = self._timeit("m.to_string()",
                           "s = EP_Person('Ryan Kelly',12345678)",
                           "r = EP_Person('Lauren Kelly',98765)",
                           "m = EP_Message('hey there','hi!!!',s,[r])")
        self.assertFasterThan(cpt,ept)

    def test_single_recipient_message_string(self):
        cpt = self._timeit("cPickle.loads(cPickle.dumps(m,-1))",
                           "s = CP_Person('Ryan Kelly',12345678)",
                           "r = CP_Person('Lauren Kelly',98765)",
                           "m = CP_Message('hey there','hi!!!',s,[r])")
        ept = self._timeit("EP_Message.from_string(m.to_string())",
                           "s = EP_Person('Ryan Kelly',12345678)",
                           "r = EP_Person('Lauren Kelly',98765)",
                           "m = EP_Message('hey there','hi!!!',s,[r])")
        self.assertFasterThan(cpt,ept)


