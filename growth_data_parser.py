"""
    Reads growth statistical data (WHO or CDC) from file and writes SQL insert statements
    Author: Anton Koba
"""

import getopt
import sys


def main(argv):
    try:
        opts, args = getopt.getopt(argv, 't:vh', ['help'])
        # print opts
        # print args
        verbose = False
        document_type = None
        input_file = None
        for option, argument in opts:
            if option in ('-h', '--help'):
                usage()
                sys.exit()
            if option == '-v':
                verbose = True
            if option == '-t':
                if argument in ('who', 'cdc'):
                    document_type = argument
                else:
                    print('Error: option -t (type) must be specified correctly')
                    usage()
                    sys.exit(2)
        if not args or len(args) != 1:
            print('Error: please specify input filename as last argument')
            usage()
            sys.exit(2)
        else:
            input_file = args[0]
        process(input_file, document_type, verbose)
    except getopt.GetoptError as err:
        print('Error:')
        print str(err)
        usage()
        sys.exit(2)


def usage():
    print """
    Usage:

    python growth_data_parser.py -t (who|cdc) [-v] inputFile.csv

    python growth_data_parser.py -h  OR
    python growth_data_parser.py --help  -  print this help message
    """


def process(input_file, document_type, verbose):
    if verbose:
        print('document type is: ' + document_type)
        print('reading input file \'' + input_file + '\': ')
    try:
        with open(input_file) as inp:
            for line in inp:
                print(line)
    except IOError as err:
        print('Error while processing file: ')
        print(str(err))

if __name__ == '__main__':
    main(sys.argv[1:])