import unittest

from btree import *
from transaction import *


class TestBtree(unittest.TestCase):
    def test_new_btree():
        to = TransationOptions()
        t = Transaction()
        b3 = Btree(1, "hello")
