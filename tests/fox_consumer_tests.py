from nose.tools import *
from fox_consumer import Consumer

def setup():
    print "SETUP!"

def teardown():
    print "TEAR DOWN!"

def test_basic():
    print "I RAN!"

class TestFoxConsumer():
    def setup(self):
        self.fox_consumer = Consumer

