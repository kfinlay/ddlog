# Data Delta Log: logging for dataset creation or update

```
usage: ddlog.py [-h] -o OUTFILE [-i [INFILE]] [-s [SCRIPT]] [-p [PROCLOG]]
                [-l [LOGPATH]] [-n [NOTE]]

Data delta log: generate a log update when data are created or changed.
Allows for separate raw and processed data files. Can compare variable lists
between those. Tries to use Markdown formatting for later document export.

optional arguments:
  -h, --help            show this help message and exit
  -o OUTFILE, --outfile OUTFILE
                        required path to processed data; if in SAS format,
                        header and variable list will be reported in log
  -i [INFILE], --infile [INFILE]
                        optional path to raw data file; if in SAS format,
                        this will enable varlist comparisons
  -s [SCRIPT], --script [SCRIPT]
                        processing script command; printed in data delta log
                        for reference (i.e., not currently executed)
  -p [PROCLOG], --proclog [PROCLOG]
                        optional path to processing log; this will be printed
                        at the end of the data delta log
  -l [LOGPATH], --logpath [LOGPATH]
                        optional path to data delta log; default is path of
                        processed data file with extension changed to '.log'
  -n [NOTE], --note [NOTE]
                        optional note; can be used to describe why and how
                        data were created or updated
```

## Public domain

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/). All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are affirming that the changes are yours to license and you are agreeing to comply with this waiver of copyright interest, or that the changes are already in the public domain.
