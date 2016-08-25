#!/usr/bin/env python

# usage: ddlog.py -i <Inputfilepath>
#                 -o <Outputfilepath>
#                 -s <processingScript>
#                 -v <Verboseoutput>

import datetime
import errno
import os
# import sys
import argparse
import hashlib


# parse the arguments
def parseargs(argv):
    parser = argparse.ArgumentParser(description='''
                                                 ddlog.py: Data Delta Log
                                                 \n
                                                 \n
                                                 Generates a log update when
                                                 data are created or changed.
                                                 ''',
                                     add_help=True)
    parser.add_argument('-i', '--input',
                        nargs='?',
                        dest='infile',
                        default='',
                        action='store',
                        help='Raw data file path'
                        )
    parser.add_argument('-o', '--output',
                        required=True,
                        nargs=1,
                        dest='outfile',
                        default='',
                        action='store',
                        help='Processed data file path'
                        )
    parser.add_argument('-s', '--script',
                        nargs='*',
                        dest='script',
                        default='',
                        action='store',
                        type=str,
                        help='Processing script command'
                        )
    parser.add_argument('-p', '--proclog',
                        nargs='*',
                        dest='proclog',
                        default='',
                        action='store',
                        help='Processing log file path'
                        )
    parser.add_argument('-l', '--logpath',
                        nargs='*',
                        dest='logpath',
                        default='',
                        action='store',
                        help='Optional path for log'
                        )
    parser.add_argument('-v', '--verbose',
                        dest="verbose",
                        default=False,
                        action="store_true"
                        )
    # parser.add_argument('--version',
    #                     action=version,
    #                     nargs='?',
    #                     dest="version",
    #                     default=0.1,
    #                     type=float
    #                     )
    # options, remainder =
    args = parser.parse_args(argv)
    return args


# identify log file path that has the same folder+basename as input
# file with log extension appended
def getlogpath(outfilename, proclog):
    if proclog == '':
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
    if infilename != '':
        logstring = (logstring +
                     'Raw file: ' + infilename + '\n'
                     )
    if script != '':
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
    # args = parseargs(['-h'])
    # args = parseargs(sys.argv[1:])
    args = parseargs(['-i', 'tests/infile.txt',
                      '-o', 'tests/outfile.txt',
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

