import unittest

from .. import transaction
from .. import btree
from .. import context

from datetime import timedelta
from ..redis import *

from dataclasses import dataclass

class TestClassVars(unittest.TestCase):
    def test_transoptions_classvars(self):
        t = transaction.TransactionOptions
    def test_btree_classvars(self):
        pass

