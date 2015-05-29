"""
    Reads growth statistical data (WHO or CDC) from file and writes SQL insert statements
    Author: Anton Koba
"""

import getopt
import string
import sys


CDC = 'cdc'
WHO = 'who'


cdc_fields_mapping = {
    'age': {
        'table': 'cdc_by_age',
        'fields': {
            'SEX': 'sex',
            '_AGEMOS1': 'agemos1',
            '_LLG1': 'llg1',
            '_MLG1': 'mlg1',
            '_SLG1': 'slg1',
            '_AGEMOS2': 'agemos2',
            '_LLG2': 'llg2',
            '_MLG2': 'mlg2',
            '_SLG2': 'slg2',
            '_AGECAT': 'agecat',
            '_LHT1': 'lht1',
            '_MHT1': 'mht1',
            '_SHT1': 'sht1',
            '_LHT2': 'lht2',
            '_MHT2': 'mht2',
            '_SHT2': 'sht2',
            '_LWT1': 'lwt1',
            '_MWT1': 'mwt1',
            '_SWT1': 'swt1',
            '_LWT2': 'lwt2',
            '_MWT2': 'mwt2',
            '_SWT2': 'swt2',
            '_LBMI1': 'lbmi1',
            '_MBMI1': 'mbmi1',
            '_SBMI1': 'sbmi1',
            '_LBMI2': 'lbmi2',
            '_MBMI2': 'mbmi2',
            '_SBMI2': 'sbmi2',
            '_LHC1': 'lhc1',
            '_MHC1': 'mhc1',
            '_SHC1': 'shc1',
            '_LHC2': 'lhc2',
            '_MHC2': 'mhc2',
            '_SHC2': 'shc2',
        }
    },
    'length': {
        'table': 'cdc_by_length',
        'fields': {
            '_LG1': 'lg1',
            '_LWLG1': 'lwlg1',
            '_MWLG1': 'mwlg1',
            '_SWLG1': 'swlg1',
            '_LG2': 'lg1',
            '_LWLG2': 'lwlg2',
            '_MWLG2': 'mwlg2',
            '_SWLG2': 'swlg2',
            '_htcat': 'htcat',
        },
    },
    'height': {
        'table': 'cdc_by_height',
        'fields': {
            '_htcat': 'htcat',
            '_HT1': 'ht1',
            '_LWHT1': 'lwht1',
            '_MWHT1': 'msht1',
            '_SWHT1': 'swht1',
            '_HT2': 'ht2',
            '_LWHT2': 'lwht2',
            '_MWHT2': 'mwht2',
            '_SWHT2': 'swht2',
        }
    },
}


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
                if argument in (CDC, WHO):
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
        print('/* document type is: ' + document_type + ' */')
        print('/* reading input file \'' + input_file + '\': */')
    try:
        with open(input_file) as inp:
            element_positions = {}
            first_line = True
            for line in inp:
                line = line.strip()
                line_elements = line.split(',')
                if first_line:
                    element_positions = get_element_positions(line_elements)
                else:
                    required_elements = get_required_elements(element_positions, line_elements, document_type, verbose)
                    if required_elements:
                        query = create_query(required_elements)
                        print(query)
                first_line = False
    except IOError as err:
        print('Error while processing file: ')
        print(str(err))


def create_query(required_elements):
    template = 'INSERT INTO {0} ({1}) VALUES ({2});'
    table_name = required_elements['table']
    fields = required_elements['fields']
    fields_list = []
    values_list = []
    for field, value in fields.items():
        if value:
            fields_list.append(field)
            values_list.append(value)
    return template.format(table_name, ','.join(fields_list), ','.join(values_list))


def get_element_positions(line_elements):
    result = {}
    index = 0
    for element in line_elements:
        result[element] = index
        index += 1
    # print(str(result))
    return result


def get_required_elements(element_positions, split_elements, document_type, verbose):
    result = {}
    map = cdc_fields_mapping if CDC == document_type else None
    denom = split_elements[0]
    # if verbose:
    #     print('/* processing denom: ' + denom + ' */')
    demon_map = map.get(denom)
    if demon_map:
        table_name = demon_map.get('table')
        fields_map = demon_map.get('fields')
        result_fields = {}
        if fields_map:
            for input_field, result_field in fields_map.iteritems():
                # print(str(input_field) + ' -> ' + str(result_field))
                position = element_positions.get(input_field)
                if position:
                    value = split_elements[position]
                    # print('value: ' + str(value))
                    result_fields[result_field] = value
                else:
                    print('/* cannot find position for element: ' + str(input_field) + ' */')
        result['table'] = table_name
        result['fields'] = result_fields
    else:
        print('/* no fields map for type ' + document_type + ' and denom ' + denom + ' */')

    # print(str(result))
    return result

if __name__ == '__main__':
    main(sys.argv[1:])