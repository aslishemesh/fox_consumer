from fox_consumer import FoxConsumer,ClassA
from fox_consumer.consumer_db_support import FoxConsumerPostgres,ClassB
from mock import Mock, MagicMock, patch
import unittest


class LocalClassB(object):

    def method1_class_b(self):
        return True

    def method2_class_b(self):
        if self.method1_class_b() == True:
            print "run method2_class_b"
        else:
            return False


class TestClassA(unittest.TestCase):
    @patch('fox_consumer.ClassA.__init__', return_value = None)
    def setUp(self, mock_class_a):
        print "setup"
        self.class_a = ClassA()
        self.class_a.class_a = "ASLI_A from test"
        self.class_a.class_b = LocalClassB()
        self.class_b = LocalClassB()

    def test_method1_class_a(self):
        print "method1-a"
        self.class_a.method1_class_a()

    def test_method2_class_a(self):
        print "method2-a"
        self.assertTrue(self.class_a.method2_class_a())

    def test_method3_class_a(self):
        print "method3-a"
        self.class_a.method3_class_a()




if __name__ == "__main__":
    unittest.main()