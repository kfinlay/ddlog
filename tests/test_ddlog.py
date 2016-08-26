# tests for ddlog.py
# haven't started writing them yet
# just testing by hand with the following
# arguments:
# args = parseargs([])
# args = parseargs(['-h'])
# args = parseargs(['-i', 'tests/data/airline.sas7bdat',
#                   '-o', 'tests/data/airline_out.sas7bdat',
#                   # '-l', 'tests/data/airline.log',
#                   '-s', 'tests/data/script.sh',
#                   '-p', 'tests/data/script.out',
#                   '-n', 'We used the wrong data last time.'
#                   ])

import unittest
from ddlog import main


class TestMain(unittest.TestCase):

    def test_main_header(self):
        assert main.header('blah') == 'blah'

    def test_main_bold(self):
        assert main.bold('blah') == 'blah'
