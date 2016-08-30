#!/usr/bin/env python

# usage: ddlog.py [-h] -o OUTFILE [-i [INFILE]] [-s [SCRIPT]] [-p [PROCLOG]]
#                 [-l [LOGPATH]] [-n [NOTE]]

# Data delta log: generate a log update when data are created or changed. Allows
# for separate raw and processed data files. Generates MD5 hashes for data
# version verification. Compares variable lists if two datasets are specified.
# Tries to use as much Markdown formatting as possible for later document
# export.

# optional arguments:
#   -h, --help            show this help message and exit
#   -o OUTFILE, --outfile OUTFILE
#                         required path to processed data; if in SAS or Stata
#                         formats, header and variable list will be reported in
#                         log
#   -i [INFILE], --infile [INFILE]
#                         optional path to raw data file; if in SAS or Stata
#                         formats, enables varlist comparisons
#   -s [SCRIPT], --script [SCRIPT]
#                         processing script command; printed in data delta log
#                         for reference (i.e., not currently executed)
#   -p [PROCLOG], --proclog [PROCLOG]
#                         optional path to processing log; this will be printed
#                         at the end of the data delta log
#   -l [LOGPATH], --logpath [LOGPATH]
#                         optional path to data delta log; default is path of
#                         processed data file with extension changed to '.log'
#   -n [NOTE], --note [NOTE]
#                         optional note; can be used to describe why and how
#                         data were created or updated

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
                                     Allows for separate raw and processed
                                     data files. Generates MD5 hashes for
                                     data version verification. Compares
                                     variable lists if two datasets are
                                     specified. Tries to use as much
                                     Markdown formatting as possible for
                                     later document export.
                                     ''',
                                     add_help=True)
    parser.add_argument('-o', '--outfile',
                        required=True,
                        nargs=1,
                        type=argparse.FileType('r'),
                        dest='outfile',
                        action='store',
                        help='''
                             required path to processed data; if
                             in SAS or Stata formats, header and
                             variable list will be reported in log
                             '''
                        )
    parser.add_argument('-i', '--infile',
                        nargs='?',
                        type=argparse.FileType('r'),
                        dest='infile',
                        action='store',
                        help='''
                             optional path to raw data file; if in
                             SAS or Stata formats, enables varlist
                             comparisons
                             '''
                        )
    parser.add_argument('-s', '--script',
                        nargs='?',
                        dest='script',
                        action='store',
                        type=str,
                        help='''
                             processing script command; printed in
                             data delta log for reference (i.e., not
                             currently executed)
                             '''
                        )
    parser.add_argument('-p', '--proclog',
                        nargs='?',
                        type=argparse.FileType('r'),
                        dest='proclog',
                        action='store',
                        help='''
                             optional path to processing log; this will
                             be printed at the end of the data delta log
                             '''
                        )
    parser.add_argument('-l', '--logpath',
                        nargs='?',
                        type=argparse.FileType('a'),
                        dest='logpath',
                        action='store',
                        help='''
                             optional path to data delta log; default is
                             path of processed data file with extension
                             changed to '.log'
                             '''
                        )
    parser.add_argument('-n', '--note',
                        nargs='?',
                        dest='note',
                        action='store',
                        type=str,
                        help='''
                             optional note; can be used to describe why
                             and how data were created or updated
                             '''
                        )
    args = parser.parse_args(argv)
    if not vars(args):
        parser.print_help()
        parser.exit(1)
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
            varlist = df['Name'].tolist()
            return h, varlist
    except:
        return None, None


# pull header and varlist from Stata format data file
def getstatavars(filename):
    try:
        with pd.read_stata(filename, iterator=True) as sr:
            # filesizestr = format(os.path.getsize(filename), ',d')
            htop = [['obs:', str(sr.nobs), sr.data_label],
                    ['vars:', str(sr.nvar), sr.time_stamp]
                    # ['size:', filesizestr, 'any notes?']]
                    ]
            dfhtop = pd.DataFrame(htop)
            htopstr = dfhtop.to_string(index=False, header=False)
            typelist = []
            for i in range(0,len(sr.dtyplist)):
                typstr = str(sr.dtyplist[i])
                typstr = typstr.lstrip("<class 'numpy.")
                typstr = typstr.rstrip("'>")
                typelist.append(typstr)
            hvarziplist = list(zip(sr.varlist, typelist, sr.fmtlist, sr.lbllist, sr.vlblist))
            dfhvar = pd.DataFrame(hvarziplist)
            dfhvar.columns = ['variable name', 'stor. type', 'disp. format', 'value label', 'variable label']
            hvarstr = dfhvar.to_string(index=False)
            h = 'Contains data from ' + filename + '\n'
            h = h + htopstr + '\n\n' + hvarstr
            varlist = sr.varlist
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
    try:
        with open(logfilename, 'r') as f:
            oldlog = f.read()
    except:
        oldlog = ''
    with open(logfilename, 'w') as f:
        f.write(logstring + oldlog)
    return 'Log updated'

if __name__ == '__main__':
    args = parseargs(sys.argv[1:])
    logpath = getlogpath(args.outfile[0].name, args.logpath)
    outmd5hash = md5sum(args.outfile[0].name)
    # get header and varlist from outfile
    if args.outfile[0].name.endswith(".xpt") or args.outfile[0].name.endswith(".sas7bdat"):
        outheader, outvarlist = getsasvars(args.outfile[0].name)
    elif args.infile.name.endswith(".dta"):
        outheader, outvarlist = getstatavars(args.outfile[0].name)
    if outvarlist is not None:
        outvarlist.sort()
    if args.infile is not None:
        inmd5hash = md5sum(args.infile.name)
        # if an infile was specified, then get the header and varlist
        if args.infile.name.endswith(".xpt") or args.infile.name.endswith(".sas7bdat"):
            inheader, invarlist = getsasvars(args.infile.name)
        elif args.infile.name.endswith(".dta"):
            inheader, invarlist = getstatavars(args.infile.name)
        # then do the varlist comparison with the outfile
        if invarlist is not None:
            invarlist.sort()
            if outvarlist is not None:
                commonvarlist = list(set.intersection(set(invarlist),
                                                      set(outvarlist)))
                commonvarlist.sort()
                outonlyvarlist = list(set(outvarlist).difference(invarlist))
                outonlyvarlist.sort()
                inonlyvarlist = list(set(invarlist).difference(outvarlist))
                inonlyvarlist.sort()
    # build the string for the new log entry (in Markdown as much as possible)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    logstring = ('\n' +
                 '# Log update\n\n' +
                 '  - Log revision date: ' + now + '\n' +
                 '  - Processed file\n' +
                 '    * Path: ' + args.outfile[0].name + '\n' +
                 '    * MD5 hash: ' + outmd5hash + '\n'
                 )
    if args.infile is not None:
        logstring = (logstring +
                     '  - Raw file\n' +
                     '    * Path: ' + args.infile.name + '\n'
                     '    * MD5 hash: ' + inmd5hash + '\n'
                     )
    if args.note is not None:
        logstring = (logstring +
                     '  - Note\n' +
                     '    * ' + args.note + '\n'
                     )
    if args.script is not None:
        logstring = (logstring +
                     '  - Processing script: ' + args.script + '\n'
                     )
    if outvarlist is not None:
        outvarnum = str(len(outvarlist))
        logstring = (logstring + '\n' +
                     '## Variable lists (case sensitive, alpha sort)' + '\n\n' +
                     '  - Variables in processed file (' + outvarnum + ')\n' +
                     '    * ' + ', '.join(outvarlist)
                     )
    if invarlist is not None:
        invarnum = str(len(invarlist))
        logstring = (logstring + '\n' +
                     '  - Variables in raw file (' + invarnum + ')\n' +
                     '    * ' + ', '.join(invarlist) + '\n'
                     )
        if outvarlist is not None:
            commonvarnum = str(len(commonvarlist))
            outonlyvarnum = str(len(outonlyvarlist))
            inonlyvarnum = str(len(inonlyvarlist))
            logstring = (logstring +
                         '  - Variables in both files (' + commonvarnum + ')\n' +
                         '    * ' + ', '.join(commonvarlist) + '\n' +
                         '  - Variables only in processed file (' + outonlyvarnum + ')\n' +
                         '    * ' + ', '.join(outonlyvarlist) + '\n' +
                         '  - Variables only in raw file (' + inonlyvarnum + ')\n' +
                         '    * ' + ', '.join(inonlyvarlist) + '\n'
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
                     '```\n' + proclog.rstrip() + '\n```\n'
                     )
    # prepend the new log entry to data log file
    result = prependlog(logpath, logstring)
    print(result)
