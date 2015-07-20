"""
    Reads growth statistical data (WHO or CDC) from file and writes SQL insert statements
    Author: Anton Koba
"""

import getopt
import sys


CDC = 'cdc'
WHO = 'who'
CDC_STATURE = 'cdc-stat'
CDC_ARMC = 'cdc-armc'
WEIGHT_VELOCITY_1MON = 'weight_velocity_1mon'
WEIGHT_VELOCITY_2MON = 'weight_velocity_2mon'


CDC_FIELDS_MAPPING = {
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
            'SEX': 'sex',
            '_LG1': 'lg1',
            '_LWLG1': 'lwlg1',
            '_MWLG1': 'mwlg1',
            '_SWLG1': 'swlg1',
            '_LG2': 'lg2',
            '_LWLG2': 'lwlg2',
            '_MWLG2': 'mwlg2',
            '_SWLG2': 'swlg2',
            '_htcat': 'htcat',
        },
    },
    'height': {
        'table': 'cdc_by_height',
        'fields': {
            'SEX': 'sex',
            '_htcat': 'htcat',
            '_HT1': 'ht1',
            '_LWHT1': 'lwht1',
            '_MWHT1': 'mwht1',
            '_SWHT1': 'swht1',
            '_HT2': 'ht2',
            '_LWHT2': 'lwht2',
            '_MWHT2': 'mwht2',
            '_SWHT2': 'swht2',
        }
    },
}


WHO_FIELDS_MAPPING = {
    'forage': {
        'table': 'who_by_age',
        'fields': {
            'sex': 'sex',
            '_agedays': 'agedays',
            '_bmi_l': 'bmil',
            '_bmi_m': 'bmim',
            '_bmi_s': 'bmis',
            '_tsf_l': 'tsf_l',
            '_tsf_m': 'tsf_m',
            '_tsf_s': 'tsf_s',
            '_ssf_l': 'ssf_l',
            '_ssf_m': 'ssf_m',
            '_ssf_s': 'ssf_s',
            '_armc_l': 'armc_l',
            '_armc_m': 'armc_m',
            '_armc_s': 'armc_s',
            '_headc_l': 'headc_l',
            '_headc_m': 'headc_m',
            '_headc_s': 'headc_s',
            '_wei_l': 'wei_l',
            '_wei_m': 'wei_m',
            '_wei_s': 'wei_s',
            '_len_l': 'len_l',
            '_len_m': 'len_m',
            '_len_s': 'len_s',
        }
    },
    'forlen': {
        'table': 'who_by_length',
        'fields': {
            'sex': 'sex',
            '_len': 'len',
            '_wfl_l': 'wfl_l',
            '_wfl_m': 'wfl_m',
            '_wfl_s': 'wfl_s',
        },
    },
}


CDC_STAT_FIELDS_MAPPING = {
    'age': {
        'table': 'cdc_stat_by_age',
        'fields': {
            'sex': 'sex',
            'agemos': 'agemos',
            'L': 'l',
            'M': 'm',
            'S': 's',
            'P3': 'p3',
            'P5': 'p5',
            'P10': 'p10',
            'P25': 'p25',
            'P50': 'p50',
            'P75': 'p75',
            'P90': 'p90',
            'P95': 'p95',
            'P97': 'p97',
        }
    },
}


CDC_ARMC_FIELDS_MAPPING = {
    'age': {
        'table': 'cdc_armc_by_age',
        'fields': {
            'Sex': 'sex',
            'Age': 'age',
            'L': 'l',
            'M': 'm',
            'S': 's',
            'P01': 'p01',
            'P1': 'p1',
            'P3': 'p3',
            'P5': 'p5',
            'P10': 'p10',
            'P25': 'p25',
            'P50': 'p50',
            'P75': 'p75',
            'P90': 'p90',
            'P95': 'p95',
            'P97': 'p97',
            'P99': 'p99',
            'P999': 'p999',
        }
    },
}


WEIGHT_VELOCITY_FIELDS_MAPPING_1MON = {
    'age': {
        'table': 'who_weight_velocity_1mon_by_age',
        'fields': {
            'Sex': 'sex',
            'Interval': 'age',
            'L': 'l',
            'M': 'm',
            'S': 's',
            'Delta': 'delta',
            '1st': 'p1',
            '3rd': 'p3',
            '5th': 'p5',
            '15th': 'p15',
            '25th': 'p25',
            '50th': 'p50',
            '75th': 'p75',
            '85th': 'p85',
            '95th': 'p95',
            '97th': 'p97',
            '99th': 'p99',
        }
    },
}

WEIGHT_VELOCITY_FIELDS_MAPPING_2MON = {
    'age': {
        'table': 'who_weight_velocity_2mon_by_age',
        'fields': {
            'Sex': 'sex',
            'agemos_from': 'agemos_from',
            'agemos_to': 'agemos_to',
            'L': 'l',
            'M': 'm',
            'S': 's',
            'Delta': 'delta',
            '1st': 'p1',
            '3rd': 'p3',
            '5th': 'p5',
            '15th': 'p15',
            '25th': 'p25',
            '50th': 'p50',
            '75th': 'p75',
            '85th': 'p85',
            '95th': 'p95',
            '97th': 'p97',
            '99th': 'p99',
        }
    },
}


TYPES_MAPPING = {
    CDC: CDC_FIELDS_MAPPING,
    WHO: WHO_FIELDS_MAPPING,
    CDC_STATURE: CDC_STAT_FIELDS_MAPPING,
    CDC_ARMC: CDC_ARMC_FIELDS_MAPPING,
    WEIGHT_VELOCITY_1MON: WEIGHT_VELOCITY_FIELDS_MAPPING_1MON,
    WEIGHT_VELOCITY_2MON: WEIGHT_VELOCITY_FIELDS_MAPPING_2MON,
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
                if argument in (CDC, WHO, CDC_STATURE, CDC_ARMC, WEIGHT_VELOCITY_1MON, WEIGHT_VELOCITY_2MON):
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

    python growth_data_parser.py -t (who|cdc|cdc-stat|cdc-armc) [-v] inputFile.csv

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
            line_number = 1
            for line in inp:
                line = line.strip()
                line_elements = line.split(',')
                if first_line:
                    element_positions = get_element_positions(line_elements)
                else:
                    required_elements = get_required_elements(element_positions, line_elements, document_type, verbose)
                    if required_elements:
                        query = create_query(required_elements, line_number)
                        print(query)
                first_line = False
                line_number += 1
    except IOError as err:
        print('Error while processing file: ')
        print(str(err))


def create_query(required_elements, num):
    template = 'INSERT INTO {0} ({1}) VALUES ({2}); /* {3} */'
    table_name = required_elements['table']
    fields = required_elements['fields']
    fields_list = []
    values_list = []
    for field, value in fields.items():
        if value:
            fields_list.append(field)
            values_list.append(value)
    return template.format(table_name, ','.join(fields_list), ','.join(values_list), num)


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
    # fields_denom_map = CDC_FIELDS_MAPPING if CDC == document_type else WHO_FIELDS_MAPPING
    fields_denom_map = TYPES_MAPPING.get(document_type)
    # print(str(element_positions))
    denom = split_elements[element_positions.get('denom', element_positions.get('_denom'))]
    # if verbose:
    #     print('/* processing denom: ' + denom + ' */')
    demon_map = fields_denom_map.get(denom)
    if demon_map:
        table_name = demon_map.get('table')
        fields_map = demon_map.get('fields')
        result_fields = {}
        if fields_map:
            for input_field, result_field in fields_map.iteritems():
                # print(str(input_field) + ' -> ' + str(result_field))
                position = element_positions.get(input_field)
                if position >= 0:
                    value = split_elements[position]
                    # print('value: ' + str(value))
                    result_fields[result_field] = value
                else:
                    print('/* cannot find position for element [' + str(input_field) + '] */')
                    print('/* positions: ' + str(element_positions) + ' */')
        result['table'] = table_name
        result['fields'] = result_fields
    else:
        print('/* no fields fields_denom_map for type ' + document_type + ' and denom ' + denom + ' */')

    # print(str(result))
    return result

if __name__ == '__main__':
    main(sys.argv[1:])