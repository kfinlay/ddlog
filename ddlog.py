#!/usr/bin/env python

# ddlog.py
#
# Data delta log: generate a log update when data are created or changed.
#
# optional arguments:
#   -h, --help            show this help message and exit
#   -o OUTFILE, --outfile OUTFILE
#                         processed data file path; the only required argument
#   -i [INFILE], --infile [INFILE]
#                         raw data file path; if in SAS format, this will
#                         enable varlist comparisons
#   -s [SCRIPT], --script [SCRIPT]
#                         processing script command; printed in data delta log
#                         for reference (i.e., not) currently executed
#   -p [PROCLOG], --proclog [PROCLOG]
#                         processing log file path; this will be printed at the
#                         end of the data delta log
#   -l [LOGPATH], --logpath [LOGPATH]
#                         optional path for data delta log; default is path of
#                         processed data file with extension changed to 'log'

import argparse
import datetime
import errno
import os
import sys
import hashlib

from io import StringIO
from sas7bdat import SAS7BDAT
import pandas as pd


# parse the arguments
def parseargs(argv):
    parser = argparse.ArgumentParser(description='''
                                     Data delta log: generate a log update
                                     when data are created or changed.
                                     ''',
                                     add_help=True)
    parser.add_argument('-o', '--outfile',
                        required=True,
                        nargs=1,
                        type=argparse.FileType('r'),
                        dest='outfile',
                        action='store',
                        help='''
                             processed data file path; the
                             only required argument
                             '''
                        )
    parser.add_argument('-i', '--infile',
                        nargs='?',
                        type=argparse.FileType('r'),
                        dest='infile',
                        action='store',
                        help='''
                             raw data file path; if in SAS format,
                             this will enable varlist comparisons
                             '''
                        )
    parser.add_argument('-s', '--script',
                        nargs='?',
                        dest='script',
                        action='store',
                        type=str,
                        help='''
                             processing script command; printed in
                             data delta log for reference (i.e., not)
                             currently executed
                             '''
                        )
    parser.add_argument('-p', '--proclog',
                        nargs='?',
                        type=argparse.FileType('r'),
                        dest='proclog',
                        action='store',
                        help='''
                             processing log file path; this will be
                             printed at the end of the data delta log
                             '''
                        )
    parser.add_argument('-l', '--logpath',
                        nargs='?',
                        type=argparse.FileType('a'),
                        dest='logpath',
                        action='store',
                        help='''
                             optional path for data delta log; default
                             is path of processed data file with extension
                             changed to 'log'
                             '''
                        )
    try:
        args = parser.parse_args(argv)
    except:
        parser.print_help()
        sys.exit(0)
    return args


# identify log file path that has the same folder+basename as input
# file with log extension appended
def getlogpath(outfilename, args_logpath):
    if args_logpath is None:
        base, ext = os.path.splitext(outfilename)
        return base + '.log'
    elif os.path.isfile(args_logpath.name) is True:
        return args_logpath.name
    else:
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT),
                                args_logpath.name)


# pull header and varlist from SAS format data file
def getsasvars(filename):
    try:
        with SAS7BDAT(filename) as f:
            # save header to h
            h = str(f.header)
            # # split the file into meta data (above) and varlist table (below)
            hmeta, hvarlist = h.split('Contents of dataset', 1)
            # drop the first line (title) of varlist table
            hvarlist = hvarlist.partition("\n")[2]
            # save header varlist to buffer/memory file
            # read in the varlist file as fixed width and infer columns
            df = pd.read_fwf(StringIO(hvarlist), colspecs='infer')
            # fvarlist.close()
            # drop the row with dashes
            df = df.drop(df.index[[0]])
            # convert all variables to string
            # I was having a hard time exporting to Stata when the type
            # was mixed (usually missings that were falsely determined to
            # be floats)
            df = df.astype(str)
            varlist = df['Name'].tolist()
            return h, varlist
    except:
        return None, None


# generate md5 hash of a file
def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, 'rb') as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()


# write update to top of log file
def prependlog(logfilename, logstring):
    with open(logfilename, 'r') as f:
        oldlog = f.read()
    with open(logfilename, 'w') as f:
        f.write(logstring + oldlog)
    return 'Log updated'

if __name__ == '__main__':
    # args = parseargs([])
    # args = parseargs(['-h'])
    # args = parseargs(sys.argv[1:])
    args = parseargs(['-i', 'tests/data/airline.sas7bdat',
                      '-o', 'tests/data/airline.sas7bdat',
                      '-l', 'tests/airline.log',
                      '-s', 'tests/script.sh',
                      '-p', 'tests/script.out'
                      ])
    logpath = getlogpath(args.outfile[0].name, args.logpath)
    outmd5hash = md5sum(args.outfile[0].name)
    # TODO(kfinlay) Need to verify that files are SAS
    # format, or skip varlist functions - currently uses try
    # for SAS files, but could provide more options for format
    # get header and varlist from outfile
    outheader, outvarlist = getsasvars(args.outfile[0].name)
    # change outvarlist for testing
    if outvarlist is not None:
        outvarlist.append('newvar1')
        outvarlist.append('newvar2')
        outvarlist.pop(0)
        outvarlist.sort()
    if args.infile is not None:
        inmd5hash = md5sum(args.infile.name)
        # if an infile was specified, then get the header and varlist
        inheader, invarlist = getsasvars(args.infile.name)
        # then do the varlist comparison with the outfile
        if invarlist is not None:
            invarlist.sort()
            if outvarlist is not None:
                commonvars = list(set.intersection(set(invarlist),
                                                   set(outvarlist)))
                commonvars.sort()
                inonlyvars = list(set(invarlist).difference(outvarlist))
                inonlyvars.sort()
                outonlyvars = list(set(outvarlist).difference(invarlist))
                outonlyvars.sort()
    # build the string for the new log entry (in Markdown as much as possible)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    logstring = ('\n' +
                 '# Log update\n\n' +
                 '  - Date log updated: ' + now + '\n' +
                 '  - Processed file\n' +
                 '    * Path: ' + args.infile.name + '\n' +
                 '    * MD5 hash: ' + outmd5hash + '\n'
                 )
    if args.infile is not None:
        logstring = (logstring +
                     '  - Raw file\n' +
                     '    * Path: ' + args.infile.name + '\n'
                     '    * MD5 hash: ' + inmd5hash + '\n'
                     )
    if args.script is not None:
        logstring = (logstring +
                     '  - Processing script: ' + args.script + '\n'
                     )
    if outvarlist is not None:
        logstring = (logstring + '\n' +
                     '## Varlists' + '\n\n' +
                     '  - Variables in processed file' + '\n' +
                     '    * ' + ', '.join(outvarlist)
                     )
    if invarlist is not None:
        logstring = (logstring + '\n' +
                     '  - Variables in raw file' + '\n' +
                     '    * ' + ', '.join(invarlist) + '\n'
                     )
        if outvarlist is not None:
            logstring = (logstring +
                         '  - Variables in both files' + '\n' +
                         '    * ' + ', '.join(commonvars) + '\n' +
                         '  - Variables only in processed file' + '\n' +
                         '    * ' + ', '.join(outonlyvars) + '\n' +
                         '  - Variables only in raw file' + '\n' +
                         '    * ' + ', '.join(inonlyvars) + '\n'
                         )
    if outheader is not None:
        logstring = (logstring + '\n' +
                     '## Header of processed file' + '\n\n' +
                     '```\n' + outheader + '\n```\n'
                     )
    if inheader is not None:
        logstring = (logstring + '\n' +
                     '## Header of raw file' + '\n\n' +
                     '```\n' + inheader + '\n```\n'
                     )
    if args.proclog is not None:
        with open(args.proclog.name, 'r') as f:
            proclog = f.read()
        logstring = (logstring + '\n' +
                     '## Log file from processing script' + '\n\n' +
                     '```\n' + proclog + '\n```\n'
                     )
    # prepend the new log entry to data log file
    result = prependlog(logpath, logstring)
    print(result)
