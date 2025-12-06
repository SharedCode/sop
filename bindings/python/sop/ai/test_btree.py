import unittest

from .. import transaction

from ..redis import *


class TestClassVars(unittest.TestCase):
    def test_transoptions_classvars(self):
        t = transaction.TransactionOptions
    def test_btree_classvars(self):
        pass

