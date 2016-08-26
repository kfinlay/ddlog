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


def getsasvars(filename):
    with SAS7BDAT(filename) as f:
        # save header to h
        h = str(f.header)
        # # split the file into meta data (above) and varlist table (below)
        hmeta, hvarlist = h.split('Contents of dataset', 1)
        # drop the first line (title) of varlist table
        hvarlist = hvarlist.partition("\n")[2]
        # # save header meta
        # text_file1 = open(fmeta, 'w')
        # text_file1.write('Path: ')
        # text_file1.write(filename)
        # text_file1.write('\n')
        # text_file1.write(hmeta)
        # text_file1.close()
        # save header varlist to buffer/memory file
        # fvarlist = StringIO.StringIO()
        # fvarlist.write(hvarlist)
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
        # # add a column with the full path of the dataset
        # # df['path'] = re.sub('/', '-', filename)
        # df['path'] = filename
        # # add columns with the number of observations and
        # # the file date modified from the header.meta
        # obs = ''
        # moddate = ''
        # for line in hmeta.splitlines():
        #     if 'row_count:' in line:
        #         junk, obs = line.split('row_count: ')
        #     if 'date_modified: ' in line:
        #         junk, moddate = line.split('date_modified: ')
        # df['obs'] = obs
        # df['moddate'] = moddate
        # # add a column for data year
        # m = yearre.search(filename)
        # try:
        #     yearstr = m.group()
        # except Exception:
        #     yearstr = ''
        # n = yearrangere.search(filename)
        # try:
        #     yearrangestr = n.group()
        # except Exception:
        #     yearrangestr = ''
        # if len(yearrangestr) > len(yearstr):
        #     df['year'] = yearrangestr
        # else:
        #     df['year'] = yearstr
        # print(df)
        # try:
        #     print('Trying to write this Stata data file:', fvarlistdta)
        #     df.to_stata(fvarlistdta)
        # # except Exception:
        # except Exception as e:
        #     print('Error writing this Stata data file:', fvarlistdta)
        #     print(df.shape)
        #     print(df.apply(lambda x: pd.lib.infer_dtype(x.values)))
        #     print(e)
        #     # import traceback; traceback.print_exc()
        # else:
        #     print('Successfully wrote this Stata data file:', fvarlistdta)
        #     print(df.shape)
        #     print(df.apply(lambda x: pd.lib.infer_dtype(x.values)))


def md5sum(filename, blocksize=65536):
    """[generate md5 hash of a file]

    [description]

    Arguments:
        filename {[type]} -- [description]

    Keyword Arguments:
        blocksize {number} -- [description] (default: {65536})

    Returns:
        [str] -- [md5hash in hex]
    """
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
    print(args.infile.name)
    print(type(args.outfile[0].name))
    print(args.outfile)
    print(args.outfile[0].name)
    print(args.script)
    print(args.proclog)
    print(args.logpath)
    print(args.verbose)
    logpath = getlogpath(args.outfile[0].name, args.logpath)
    print(logpath)
    md5hash = md5sum(args.outfile[0].name)
    print(md5hash)
    logstring = genlogstring(logpath, args.infile.name, args.outfile[0].name,
                             args.script, md5hash)
    print(logstring)
    result = appendlog(logpath, logstring)
    print(result)
    header, varlist = getsasvars('tests/data/airline.sas7bdat')
    print(header)
    print(type(varlist))
    print(varlist)
