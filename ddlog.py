#!/usr/bin/env python

# ddlog.py: Data Delta Log Generates a log update when data are created or
# changed.

# optional arguments:
#   -h, --help            show this help message and exit
#   -i [INFILE], --input [INFILE]
#                         Raw data file path
#   -o OUTFILE, --output OUTFILE
#                         Processed data file path
#   -s [SCRIPT [SCRIPT ...]], --script [SCRIPT [SCRIPT ...]]
#                         Processing script command
#   -p [PROCLOG [PROCLOG ...]], --proclog [PROCLOG [PROCLOG ...]]
#                         Processing log file path
#   -l [LOGPATH [LOGPATH ...]], --logpath [LOGPATH [LOGPATH ...]]
#                         Optional path for log
#   -v, --verbose

import argparse
import datetime
import errno
import os
import sys
import hashlib


# parse the arguments
def parseargs(argv):
    parser = argparse.ArgumentParser(description='''
                                     Data delta log: generate a log update
                                     when data are created or changed.
                                     ''',
                                     add_help=True)
    parser.add_argument('-i', '--input',
                        nargs='?',
                        type=argparse.FileType('r'),
                        dest='infile',
                        action='store',
                        help='raw data file path'
                        )
    parser.add_argument('-o', '--output',
                        required=True,
                        nargs=1,
                        type=argparse.FileType('a'),
                        dest='outfile',
                        action='store',
                        help='processed data file path'
                        )
    parser.add_argument('-s', '--script',
                        nargs='?',
                        dest='script',
                        action='store',
                        type=str,
                        help='processing script command'
                        )
    parser.add_argument('-p', '--proclog',
                        nargs='?',
                        type=argparse.FileType('r'),
                        dest='proclog',
                        action='store',
                        help='processing log file path'
                        )
    parser.add_argument('-l', '--logpath',
                        nargs='?',
                        type=argparse.FileType('a'),
                        dest='logpath',
                        action='store',
                        help='optional path for log'
                        )
    parser.add_argument('-v', '--verbose',
                        dest="verbose",
                        default=False,
                        action='store_true',
                        help='increase output verbosity'
                        )
    # parser.add_argument('--version',
    #                     action=version,
    #                     nargs='?',
    #                     dest="version",
    #                     default=0.1,
    #                     type=float
    #                     )
    try:
        args = parser.parse_args(argv)
    except:
        parser.print_help()
        sys.exit(0)
    return args


# identify log file path that has the same folder+basename as input
# file with log extension appended
def getlogpath(outfilename, proclog):
    if proclog is None:
        base, ext = os.path.splitext(outfilename)
        return base + '.log'
    elif os.path.isfile(proclog) is True:
        return proclog
    else:
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT),
                                proclog)


# generate md5 hash of a file
def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()


# gen update for log file
def genlogstring(logfilename, infilename, outfilename, script, md5hash):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    logstring = ('\n' +
                 '**********\n' +
                 'Log update\n' +
                 '**********\n\n' +
                 'Processed file: ' + outfilename + '\n' +
                 'Date log updated: ' + now + '\n' +
                 'MD5 hash of processed file: ' + md5hash + '\n' +
                 '\n'
                 )
    if infilename is not None:
        logstring = (logstring +
                     'Raw file: ' + infilename + '\n'
                     )
    if script is not None:
        logstring = (logstring +
                     'Processing script: ' + script + '\n'
                     )
    return logstring


# append update to log file
def appendlog(logfilename, logstring):
    with open(logfilename, 'a') as f:
        f.write(logstring)
    return 'Log updated'

if __name__ == '__main__':
    # args = parseargs([])
    # args = parseargs(['-h'])
    # args = parseargs(sys.argv[1:])
    args = parseargs(['-i', 'tests/infile.txt',
                      # '-o', 'tests/outfile.txt',
                      '-o', 'tests/outfile3.txt',
                      '-s', 'tests/script.sh',
                      '-v'])
    print(args.infile)
    print(args.outfile)
    print(args.outfile[0])
    print(args.script)
    print(args.proclog)
    print(args.logpath)
    print(args.verbose)
    logpath = getlogpath(args.outfile[0], args.logpath)
    print(logpath)
    md5hash = md5sum(args.outfile[0])
    print(md5hash)
    logstring = genlogstring(logpath, args.infile, args.outfile[0],
                             args.script[0], md5hash)
    print(logstring)
    result = appendlog(logpath, logstring)
    print(result)
