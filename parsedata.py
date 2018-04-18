# parsedata.py - program to parse counters output by fs-drift.py
# into .csv file format so they can be graphed easily.
# this only handles a single process output at this time
# take output of fs-drift.py and make it this program's input

import string
import sys

headers = []
hdr_record = ''
counter_record = ''
counter_sections = 0
counter_begin = True
for line in sys.stdin.readlines():
    tokens = string.split(string.strip(line))
    if len(tokens) < 2:
        continue  # skip lines that couldn't be counters
    if tokens[0] == 'elapsed':  # if beginning of a counter section
        counter_sections += 1  # count how many counter sections
        counter_begin = True   # we're in the counter section
        counter_record = ''

    elif (len(tokens) > 3) and \
         (tokens[1] == '=') and (tokens[2] == 'total') and (tokens[3] == 'errors'):
        # end of a counter section
        # if first one, output headers for .csv
        # output next counter record
        counter_begin = False
        if counter_sections == 1:
            for x in headers:
                hdr_record += x + ','
            print(hdr_record)
        print(counter_record)

    if (counter_sections == 1) and counter_begin and (tokens[1] == '='):
        # for first section, record counter descriptions on right of '='
        colhdr_tokens = tokens[2:]
        colhdr = ''
        for x in colhdr_tokens:
            colhdr += x + ' '
        headers.append(colhdr)

    if counter_begin and (tokens[1] == '='):  # if it's a counter
        counter_record += tokens[0] + ','
