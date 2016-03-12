__author__ = 'renienj'

#!/usr/bin/env python

import sys

def read_input(file):
    for line in file:
        # split the line into words
        yield line.split('\r')

# Add default count to each word
def name_count(data):
    for words in data:
        for word in words:
            value = word.split('\t')[-1]
            print '%s%s%d' % (value, '\t', 1)

def feature_count(data):
    for words in data:
        for word in words:
            value = word.split('\t')[-1]
            print '%s%s%d' % (value, '\t', 1)


def main(argv):
    data = read_input(sys.stdin)
    name_count(data)

if __name__ == "__main__":
    main(sys.argv)
