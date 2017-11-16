import codecs
import numpy as np
import time
from datetime import date
from datetime import timedelta

table_definition = [
    {'table': 'csdc_h_sec_tran_tst',
     'columns': [
         {'name': 'TRAD_NBR', 'type': 'SERIAL', 'range': [1, 99999999999]},
         {'name': 'TRAD_DIRC', 'type': 'STRING', 'enum': ['B', 'S']},
         {'name': 'SHDR_ACCT', 'type': 'NUMBER', 'range': [100000000, 300000000]},
         {'name': 'SHDR_TYPE', 'type': 'STRING', 'enum': ['0']},
         {'name': 'MST_SEAT_CDE', 'type': 'STRING', 'enum': ['000000']},
         {'name': 'SEAT_CDE', 'type': 'STRING', 'enum': ['000000']},
         {'name': 'SEC_CDE', 'type': 'STRING', 'expr': "stocks[int({rand})]['stock_id']", 'range': [239, 1587],
          'save_rand': True},
         {'name': 'CAP_TYPE', 'type': 'STRING', 'enum': ['JJ', 'PT', 'PZ', 'GZ']},
         {'name': 'NEGT_TYPE', 'type': 'STRING', 'enum': ['0']},
         {'name': 'EQUT_TYPE', 'type': 'STRING', 'enum': ['DF', 'DX', 'HL']},
         {'name': 'EQUT_YEARS', 'type': 'NUMBER', 'range': [1, 10]},
         {'name': 'TRANS_TYPE', 'type': 'STRING', 'enum': ['00A']},
         {'name': 'TRANS_DATE', 'type': 'DATE', 'expr': 'data_date'},
         {'name': 'TRANS_VOL', 'type': 'NUMBER', 'range': [1, 9999999],
          'dist': {'type': 'normal', 'mu': 100, 'sigma': 50}},
         {'name': 'THIS_BAL', 'type': 'NUMBER', 'range': [0, 0]},
         {'name': 'SRC_TRAN_PRC', 'type': 'NUMBER', 'expr': "stocks[int(save_rand)]['close_price'] * {rand}",
          'range': [0.9, 1.1], 'format': '%.2f'},
         {'name': 'TRAN_PRC', 'type': 'NUMBER', 'copy': 'SRC_TRAN_PRC'},
         {'name': 'TRAD_TIME', 'type': 'TIME', 'format': 'HHMMSS', 'range': [93000, 113100, 130000, 150100]},
         {'name': 'APLY_NBR', 'type': 'STRING', 'enum': ['0000000000000000000']},
         {'name': 'DATA_DATE', 'type': 'DATE', 'expr': 'data_date'},
     ]},
    {'table': 'csdc_s_sec_tran_tst',
     'columns': [
         {'name': 'SEC_CDE', 'type': 'STRING', 'expr': "stocks[int({rand})]['stock_id']", 'range': [0, 1348],
         'save_rand': True},
         {'name': 'SEAT_CDE', 'type': 'STRING', 'enum': ['000000']},
         {'name': 'SHDR_ACCT', 'type': 'NUMBER', 'range': [400000000, 600000000]},
         {'name': 'TRAD_DIRC', 'type': 'STRING', 'enum': ['B', 'S']},
         {'name': 'CNTR_NBR', 'type': 'SERIAL', 'range': [1, 99999999999], 'format': '%024d'},
         {'name': 'TRAD_NBR', 'type': 'SERIAL', 'range': [1, 99999999999], 'format': '%018d'},
         {'name': 'TRANS_VOL', 'type': 'NUMBER', 'range': [1, 9999999],
          'dist': {'type': 'normal', 'mu': 100, 'sigma': 50}},
         {'name': 'TRAN_PRC', 'type': 'NUMBER', 'expr': "stocks[int(save_rand)]['close_price'] * {rand}",
          'range': [0.9, 1.1], 'format': '%.2f'},
         {'name': 'TRAD_TIME', 'type': 'TIME', 'format': 'HHMMSS', 'range': [93000, 113100, 130000, 150100]},
         {'name': 'CMT_FUNC', 'type': 'STRING', 'enum': ['0']},
         {'name': 'CMT_BIZ', 'type': 'STRING', 'enum': ['0']},
         {'name': 'CMT_BOARD', 'type': 'STRING', 'enum': ['0']},
         {'name': 'DATA_DATE', 'type': 'DATE', 'expr': 'data_date'},
     ]}
]


def get_sh_stocks():
    stocks = []
    f = codecs.open('mktdt00.txt', encoding='gbk')

    lines = f.readlines()

    for i in range(len(lines)):
        if i == 0 or i == len(lines) - 1:
            continue
        fields = lines[i].split('|')
        stocks.append((fields[1], fields[2]))

    return stocks


def get_sz_stocks():
    stocks = []
    f = codecs.open('sjssj.txt', encoding='utf8')

    lines = f.readlines()

    for i in range(len(lines)):
        fields = lines[i].split('|')
        stocks.append((fields[0], fields[1]))

    return stocks


def gen_table_data(params):
    output = codecs.open(params['data_file'], mode='w', encoding='utf8')

    value_params = {
        'data_date': params['data_date'],
        'stocks': params['stocks'],
    }

    table = filter(lambda x: x['table'] == params['table'], table_definition)[0]
    columns = table['columns']

    count = 0
    while count < params['row_count']:
        values = {}
        for column in columns:
            gen_column_value(column, values, value_params)

        # print values
        value_str = ''
        for column in columns:
            value_str = value_str + '|' + values[column['name']]
        value_str += '\n'
        output.write(value_str[1:])
        count += 1

    output.close()


def gen_column_value(column, values, value_params):
    def gen_string(column, values, value_params):
        # print 'generating string value for column %s' % column['name']
        if 'enum' in column:
            values[column['name']] = column['enum'][int(len(column['enum']) * np.random.random())]
        elif 'expr' in column:
            if column['expr'].find('{rand}') != -1:
                if 'range' in column:
                    range_section = int(len(column['range']) / 2 * np.random.random())
                    rand = column['range'][range_section * 2] + \
                           (column['range'][range_section * 2 + 1] - column['range'][range_section * 2]) \
                           * np.random.random()
                else:
                    rand = np.random.random()
                if 'save_rand' in column:
                    value_params['save_rand'] = rand
                expr = column['expr'].replace('{rand}', str(rand))
                values[column['name']] = str(eval(expr, value_params))
            else:
                values[column['name']] = str(eval(column['expr'], value_params))

    def gen_number(column, values, value_params):
        # print 'generating number value for column %s' % column['name']
        value = 0
        if 'expr' in column:
            if column['expr'].find('{rand}') != -1:
                if 'range' in column:
                    range_section = int(len(column['range']) / 2 * np.random.random())
                    rand = column['range'][range_section * 2] + \
                           (column['range'][range_section * 2 + 1] - column['range'][range_section * 2]) \
                           * np.random.random()
                else:
                    rand = np.random.random()
                if 'save_rand' in column:
                    value_params['save_rand'] = rand
                expr = column['expr'].replace('{rand}', str(rand))
            else:
                expr = column['expr']

            value = eval(expr, value_params)
        elif 'range' in column:
            range_section = int(len(column['range']) / 2 * np.random.random())
            if 'dist' not in column:
                rand = column['range'][range_section * 2] + \
                       (column['range'][range_section * 2 + 1] - column['range'][range_section * 2]) \
                       * np.random.random()
            else:
                rand = max(min(np.random.normal(column['dist']['mu'], column['dist']['sigma']),
                               column['range'][range_section + 1]),
                           column['range'][range_section])

            if 'save_rand' in column:
                value_params['save_rand'] = rand

            value = int(rand)

        if 'format' in column:
            value = column['format'] % value

        values[column['name']] = str(value)

    def gen_date(column, values, value_params):
        # print 'generating date value for column %s' % column['name']
        if 'expr' in column:
            values[column['name']] = str(eval(column['expr'], value_params))

    def gen_time(column, values, value_params):
        # print 'generating time value for column %s' % column['name']
        if 'range' in column:
            range_section = int(len(column['range']) / 2 * np.random.random())
            start_time = column['range'][range_section * 2]
            end_time = column['range'][range_section * 2 + 1]
            start_tm = time.mktime(time.strptime('20000101' + str(start_time / 10000) + str(start_time % 10000 / 100) +
                                                 str(start_time % 100), '%Y%m%d%H%M%S'))
            end_tm = time.mktime(time.strptime('20000101' + str(end_time / 10000) + str(end_time % 10000 / 100) +
                                               str(end_time % 100), '%Y%m%d%H%M%S'))
            gen_time = time.localtime(int(start_tm + (end_tm - start_tm) * np.random.random()))
            values[column['name']] = str(gen_time.tm_hour * 10000 + gen_time.tm_min * 100 + gen_time.tm_sec)

    def gen_serial(column, values, value_params):
        last_serial = value_params.get('last_serial', column['range'][0] - 1)
        value_format = column['format'] if 'format' in column else '%d'
        values[column['name']] = value_format % (last_serial + 1)
        value_params['last_serial'] = last_serial + 1

    generators = {
        'STRING': gen_string,
        'NUMBER': gen_number,
        'DATE': gen_date,
        'TIME': gen_time,
        'SERIAL': gen_serial,
    }

    if 'copy' in column:
        values[column['name']] = values.get(column['copy'], 'Unknown')
    else:
        generators[column['type']](column, values, value_params)


if __name__ == '__main__':
    sh_stocks = get_sh_stocks()
    sz_stocks = get_sz_stocks()

    # for s in stocks:
    #     print s[0], s[1]

    cur_date = date(2016, 9, 1)
    date_list = [cur_date + timedelta(days=i) for i in range(1)]

    for cur_date in date_list:
        # gen_table_data({
        #     'table': 'csdc_h_sec_tran_tst',
        #     'row_count': 10000,
        #     'data_date': str(cur_date),
        #     'data_file': r'c:\tmp\csdc_h_sec_tran_tst.dat',
        #     'stocks': [{'stock_id': stock[0], 'close_price': np.random.random() * 99 + 1} for stock in sh_stocks],
        # })
        gen_table_data({
            'table': 'csdc_s_sec_tran_tst',
            'row_count': 1000000,
            'data_date': str(cur_date),
            'data_file': r'c:\tmp\csdc_s_sec_tran_tst.dat',
            'stocks': [{'stock_id': stock[0], 'close_price': np.random.random() * 99 + 1} for stock in sz_stocks],
        })
