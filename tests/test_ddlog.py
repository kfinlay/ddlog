import unittest
from ddlog import main


class TestMain(unittest.TestCase):

    def test_main_header(self):
        assert main.header('blah') == 'blah'

    def test_main_bold(self):
        assert main.bold('blah') == 'blah'
