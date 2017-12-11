import codecs
import numpy as np
import time
from datetime import date
from datetime import timedelta

table_definition = [
    {'table': 'csdc_h_sec_tran_tst',
     'columns': [
         {'id': 1, 'name': 'TRAD_NBR', 'type': 'NUMBER', 'expr': '{serial}', 'range': [1, 99999999999]},
         {'id': 2, 'name': 'TRAD_DIRC', 'type': 'STRING', 'enum': ['B', 'S']},
         {'id': 3, 'name': 'SHDR_ACCT', 'type': 'NUMBER', 'range': [10000, 30000], 'format': '%020d'},
         {'id': 4, 'name': 'SHDR_TYPE', 'type': 'STRING', 'enum': ['0']},
         {'id': 5, 'name': 'MST_SEAT_CDE', 'type': 'STRING', 'enum': ['000000']},
         {'id': 6, 'name': 'SEAT_CDE', 'type': 'STRING', 'enum': ['000000']},
         {'id': 7, 'name': 'SEC_CDE', 'type': 'STRING', 'expr': "stocks[int({rand})]['stock_id']",
          'range': [239, 1586]},
         {'id': 8, 'name': 'CAP_TYPE', 'type': 'STRING', 'enum': ['JJ', 'PT', 'PZ', 'GZ']},
         {'id': 9, 'name': 'NEGT_TYPE', 'type': 'STRING', 'enum': ['0']},
         {'id': 10, 'name': 'EQUT_TYPE', 'type': 'STRING', 'enum': ['DF', 'DX', 'HL']},
         {'id': 11, 'name': 'EQUT_YEARS', 'type': 'NUMBER', 'range': [1, 10]},
         {'id': 12, 'name': 'TRANS_TYPE', 'type': 'STRING', 'enum': ['00A']},
         {'id': 13, 'name': 'TRANS_DATE', 'type': 'DATE', 'expr': 'data_date'},
         {'id': 14, 'name': 'TRANS_VOL', 'type': 'NUMBER', 'range': [1, 9999999],
          'dist': {'type': 'normal', 'mu': 100, 'sigma': 50}},
         {'id': 15, 'name': 'THIS_BAL', 'type': 'NUMBER', 'range': [0, 0]},
         {'id': 16, 'name': 'SRC_TRAN_PRC', 'type': 'NUMBER',
          'expr': "stocks[int({last_rand})]['close_price'] * {rand}",
          'range': [0.9, 1.1], 'format': '%.2f'},
         {'id': 17, 'name': 'TRAN_PRC', 'type': 'NUMBER', 'copy': 'SRC_TRAN_PRC', 'format': '%.2f'},
         {'id': 18, 'name': 'TRAD_TIME', 'type': 'TIME', 'tm_format': 'HHMMSS',
          'range': [93000, 113100, 130000, 150100]},
         {'id': 19, 'name': 'APLY_NBR', 'type': 'STRING', 'enum': ['0000000000000000000']},
         {'id': 20, 'name': 'DATA_DATE', 'type': 'DATE', 'expr': 'data_date'},
     ]},
    {'table': 'csdc_s_sec_tran_tst',
     'columns': [
         {'id': 1, 'name': 'TRAD_DATE', 'type': 'DATE', 'expr': 'data_date'},
         {'id': 2, 'name': 'SEC_CDE', 'type': 'STRING', 'expr': "stocks[int({rand})]['stock_id']", 'range': [0, 1347]},
         {'id': 9, 'name': 'TRAN_PRC', 'type': 'NUMBER', 'expr': "stocks[int({last_rand})]['close_price']",
          'range': [0.9, 1.1], 'format': '%.2f'},
         {'id': 3, 'name': 'SEAT_CDE', 'type': 'STRING', 'enum': ['000000']},
         {'id': 4, 'name': 'SHDR_ACCT', 'type': 'NUMBER', 'range': [40000000, 50000000], 'format': '%010d'},
         {'id': 5, 'name': 'TRAD_DIRC', 'type': 'STRING', 'enum': ['B', 'S']},
         {'id': 6, 'name': 'CNTR_NBR', 'type': 'NUMBER', 'expr': '{serial}', 'range': [1, 99999999999],
          'format': '%024d'},
         {'id': 7, 'name': 'TRAD_NBR', 'type': 'NUMBER', 'expr': 'CNTR_NBR', 'format': '%018d'},
         {'id': 8, 'name': 'TRANS_VOL', 'type': 'NUMBER', 'expr': '1',
          'dist': {'type': 'normal', 'mu': 100, 'sigma': 50}},
         {'id': 10, 'name': 'TRAD_TIME', 'type': 'TIME', 'tm_format': 'HHMMSS',
          'range': [93000, 113100, 130000, 150100]},
         {'id': 11, 'name': 'CMT_FUNC', 'type': 'STRING', 'enum': ['0']},
         {'id': 12, 'name': 'CMT_BIZ', 'type': 'STRING', 'enum': ['0']},
         {'id': 13, 'name': 'CMT_BOARD', 'type': 'STRING', 'enum': ['0']},
         {'id': 14, 'name': 'DATA_DATE', 'type': 'DATE', 'expr': 'data_date'},
     ]},
    {'table': 'sse_sec_idx_quot_tst',
     'columns': [
         {'id': 1, 'name': 'SEC_CDE', 'type': 'STRING', 'expr': "stocks[int({serial})]['stock_id']",
          'range': [239, 1586]},
         {'id': 2, 'name': 'SEC_NAME', 'type': 'STRING', 'expr': "stocks[int({last_serial})]['stock_name']"},
         {'id': 3, 'name': 'AGO_CLS_PRC', 'type': 'NUMBER', 'expr': "stocks[int({last_serial})]['close_price']",
          'format': '%.2f'},
         {'id': 4, 'name': 'TS_OPN_PRC', 'type': 'NUMBER', 'expr': "AGO_CLS_PRC", 'format': '%.2f'},
         {'id': 8, 'name': 'LTST_PRC', 'type': 'NUMBER', 'expr': "TS_OPN_PRC * {rand}", 'range': [0.9, 1.1],
          'format': '%.2f'},
         {'id': 9, 'name': 'TRAD_VOL', 'type': 'NUMBER', 'range': [1, 10000], 'format': '%d'},
         {'id': 5, 'name': 'TS_TRAD_AMT', 'type': 'NUMBER', 'expr': "TRAD_VOL * LTST_PRC", 'format': '%.2f'},
         {'id': 6, 'name': 'TOP_PRC', 'type': 'NUMBER', 'expr': 'max(AGO_CLS_PRC, LTST_PRC, {rand}*AGO_CLS_PRC)',
          'range': [0.9, 1.1], 'format': '%.2f'},
         {'id': 7, 'name': 'MIN_PRC', 'type': 'NUMBER', 'expr': 'min(AGO_CLS_PRC, LTST_PRC, {rand}*AGO_CLS_PRC)',
          'range': [0.9, 1.1], 'format': '%.2f'},
         {'id': 10, 'name': 'CRENT_BUY_PRC', 'type': 'NUMBER', 'copy': "LTST_PRC", 'format': '%.2f'},
         {'id': 11, 'name': 'CRENT_SAL_PRC', 'type': 'NUMBER', 'copy': "LTST_PRC", 'format': '%.2f'},
         {'id': 12, 'name': 'PE', 'type': 'NUMBER', 'range': [100, 100]},
         {'id': 13, 'name': 'APPBY_QTT_ONE', 'type': 'NUMBER', 'copy': 'TRAD_VOL'},
         {'id': 14, 'name': 'APPBY_PRC_TWO', 'type': 'NUMBER', 'expr': 'LTST_PRC - 0.01', 'format': '%.2f'},
         {'id': 15, 'name': 'APPBY_QTT_TWO', 'type': 'NUMBER', 'range': [1, 100000]},
         {'id': 16, 'name': 'APPBY_PRC_THREE', 'type': 'NUMBER', 'expr': 'LTST_PRC - 0.02', 'format': '%.2f'},
         {'id': 17, 'name': 'APPBY_QTT_THREE', 'type': 'NUMBER', 'range': [1, 100000]},
         {'id': 18, 'name': 'APPSL_QTT_ONE', 'type': 'NUMBER', 'copy': 'TRAD_VOL'},
         {'id': 19, 'name': 'APPSL_PRC_TWO', 'type': 'NUMBER', 'expr': 'LTST_PRC + 0.01', 'format': '%.2f'},
         {'id': 20, 'name': 'APPSL_QTT_TWO', 'type': 'NUMBER', 'range': [1, 100000]},
         {'id': 21, 'name': 'APPSL_PRC_THREE', 'type': 'NUMBER', 'expr': 'LTST_PRC + 0.02', 'format': '%.2f'},
         {'id': 22, 'name': 'APPSL_QTT_THREE', 'type': 'NUMBER', 'range': [1, 100000]},
         {'id': 23, 'name': 'APPBY_PRC_FR', 'type': 'NUMBER', 'expr': 'LTST_PRC - 0.03', 'format': '%.2f'},
         {'id': 24, 'name': 'APPBY_QTT_FR', 'type': 'NUMBER', 'range': [1, 100000]},
         {'id': 25, 'name': 'APPBY_PRC_FV', 'type': 'NUMBER', 'expr': 'LTST_PRC - 0.04', 'format': '%.2f'},
         {'id': 26, 'name': 'APPBY_QTT_FV', 'type': 'NUMBER', 'range': [1, 100000]},
         {'id': 27, 'name': 'APPSL_PRC_FR', 'type': 'NUMBER', 'expr': 'LTST_PRC + 0.03', 'format': '%.2f'},
         {'id': 28, 'name': 'APPSL_QTT_FR', 'type': 'NUMBER', 'range': [1, 100000]},
         {'id': 29, 'name': 'APPSL_PRC_FV', 'type': 'NUMBER', 'expr': 'LTST_PRC + 0.04', 'format': '%.2f'},
         {'id': 30, 'name': 'APPSL_QTT_FV', 'type': 'NUMBER', 'range': [1, 100000]},
     ]},
    {'table': 'szse_sec_td_end_quot_tst',
     'columns': [
         {'id': 1, 'name': 'EXCH', 'type': 'STRING', 'enum': ['1']},
         {'id': 2, 'name': 'SEC_CDE', 'type': 'STRING', 'expr': "stocks[{serial}]['stock_id']", 'range': [0, 1347]},
         {'id': 3, 'name': 'SEC_ABBR', 'type': 'STRING', 'expr': "stocks[{last_serial}]['stock_name']"},
         {'id': 4, 'name': 'CRNC_CDE', 'type': 'STRING', 'enum': ['CNY']},
         {'id': 5, 'name': 'YDY_CLS_PRC', 'type': 'NUMBER', 'expr': "stocks[{last_serial}]['close_price']",
          'format': '%.2f'},
         {'id': 6, 'name': 'OPN_PRC', 'type': 'NUMBER', 'expr': "YDY_CLS_PRC", 'format': '%.2f'},
         {'id': 7, 'name': 'CLS_PRC', 'type': 'NUMBER', 'expr': "YDY_CLS_PRC * {rand}", 'range': [0.9, 1.1],
          'format': '%.2f'},
         {'id': 8, 'name': 'TOP_PRC', 'type': 'NUMBER', 'expr': "max(YDY_CLS_PRC, CLS_PRC, YDY_CLS_PRC * {rand})",
          'range': [0.9, 1.1], 'format': '%.2f'},
         {'id': 9, 'name': 'MIN_PRC', 'type': 'NUMBER', 'expr': "min(YDY_CLS_PRC, CLS_PRC, YDY_CLS_PRC * {rand})",
          'range': [0.9, 1.1], 'format': '%.2f'},
         {'id': 10, 'name': 'TRAD_VOL', 'type': 'NUMBER', 'range': [100000, 10000000], 'format': '%d'},
         {'id': 11, 'name': 'TRAD_AMT', 'type': 'NUMBER', 'expr': "TRAD_VOL * (TOP_PRC - MIN_PRC)/2", 'format': '%.2f'},
         {'id': 12, 'name': 'IF_TDHL', 'type': 'STRING', 'enum': ['0']},
         {'id': 13, 'name': 'IF_EXR_EXD', 'type': 'STRING', 'enum': ['0']},
         {'id': 14, 'name': 'LDO_INDC', 'type': 'STRING', 'enum': ['0']},
         {'id': 15, 'name': 'VBR_RANG', 'type': 'NUMBER', 'expr': "(TOP_PRC-MIN_PRC)/OPN_PRC", 'format': '%.2f'},
         {'id': 16, 'name': 'CHG_RATE_DGRE', 'type': 'NUMBER', 'expr': "abs(CLS_PRC/OPN_PRC-1)", 'format': '%.2f'},
         {'id': 17, 'name': 'TNOV_RATE1', 'type': 'NUMBER', 'expr': "{rand}", 'range': [0.01, 0.7], 'format': '%.2f'},
         {'id': 18, 'name': 'TNOV_RATE2', 'type': 'NUMBER', 'expr': "{rand}", 'range': [0.01, 0.7], 'format': '%.2f'},
         {'id': 19, 'name': 'CRNC_EXCH_RT', 'type': 'NUMBER', 'range': [1, 1]},
         {'id': 20, 'name': 'DATA_DATE', 'type': 'DATE', 'expr': 'data_date'},
     ]},
    {'table': 'sec_quot_3s_szse_s_tst',
     'columns': [
         {'id': 1, 'name': 'SEC_CODE', 'type': 'STRING', 'expr': "stocks[int({rand})]['stock_id']", 'range': [0, 1347]},
         {'id': 4, 'name': 'PRE_CLOSE_PRICE', 'type': 'NUMBER', 'expr': "stocks[int({last_rand})]['close_price']",
          'format': '%.2f'},
         {'id': 2, 'name': 'QUOT_SEQ', 'type': 'NUMBER', 'expr': '{serial}', 'range': [1, 99999999999]},
         {'id': 3, 'name': 'QUOT_TIME', 'type': 'TIME', 'tm_format': 'HHMMSS',
          'range': [93000, 113100, 130000, 150100]},
         {'id': 5, 'name': 'OPEN_PRICE', 'type': 'NUMBER', 'copy': 'PRE_CLOSE_PRICE', 'format': '%.2f'},
         {'id': 6, 'name': 'TRADE_VOL', 'type': 'NUMBER', 'range': [1, 10000],
          'dist': {'type': 'normal', 'mu': 100, 'sigma': 1000}},
         {'id': 7, 'name': 'TRADE_AMT', 'type': 'NUMBER', 'expr': '100'},
         {'id': 8, 'name': 'TRADE_CNT', 'type': 'NUMBER', 'expr': '100'},
         {'id': 9, 'name': 'TRADE_VOL_M', 'type': 'NUMBER', 'expr': '100'},
         {'id': 10, 'name': 'TRADE_AMT_M', 'type': 'NUMBER', 'expr': '100'},
         {'id': 11, 'name': 'TRADE_CNT_M', 'type': 'NUMBER', 'expr': '100'},
         {'id': 12, 'name': 'HIGH_PRICE', 'type': 'NUMBER', 'expr': 'OPEN_PRICE * {rand}', 'range': [0.9, 1.1],
          'format': '%.2f'},
         {'id': 13, 'name': 'LOW_PRICE', 'type': 'NUMBER', 'expr': 'max(HIGH_PRICE * {rand}, OPEN_PRICE * 0.9)',
          'range': [0.9, 0.99], 'format': '%.2f'},
         {'id': 14, 'name': 'CURR_PRICE', 'type': 'NUMBER', 'expr': 'HIGH_PRICE',
          'format': '%.2f'},
         {'id': 15, 'name': 'ORDER_HIGH_PRICE', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 16, 'name': 'ORDER_LOW_PRICE', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 17, 'name': 'PE_RATE_1', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 18, 'name': 'PE_RATE_2', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 19, 'name': 'PRICE_DIFF_1', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 20, 'name': 'PRICE_DIFF_2', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 21, 'name': 'POS_VOL', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 22, 'name': 'BUY_PRICE_1', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 23, 'name': 'BUY_VOL_1', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 24, 'name': 'BUY_PRICE_2', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 25, 'name': 'BUY_VOL_2', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 26, 'name': 'BUY_PRICE_3', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 27, 'name': 'BUY_VOL_3', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 28, 'name': 'BUY_PRICE_4', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 29, 'name': 'BUY_VOL_4', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 30, 'name': 'BUY_PRICE_5', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 31, 'name': 'BUY_VOL_5', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 32, 'name': 'BUY_PRICE_6', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 33, 'name': 'BUY_VOL_6', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 34, 'name': 'BUY_PRICE_7', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 35, 'name': 'BUY_VOL_7', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 36, 'name': 'BUY_PRICE_8', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 37, 'name': 'BUY_VOL_8', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 38, 'name': 'SELL_PRICE_1', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 39, 'name': 'SELL_VOL_1', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 40, 'name': 'SELL_PRICE_2', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 41, 'name': 'SELL_VOL_2', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 42, 'name': 'SELL_PRICE_3', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 43, 'name': 'SELL_VOL_3', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 44, 'name': 'SELL_PRICE_4', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 45, 'name': 'SELL_VOL_4', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 46, 'name': 'SELL_PRICE_5', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 47, 'name': 'SELL_VOL_5', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 48, 'name': 'SELL_PRICE_6', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 49, 'name': 'SELL_VOL_6', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 50, 'name': 'SELL_PRICE_7', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 51, 'name': 'SELL_VOL_7', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 52, 'name': 'SELL_PRICE_8', 'type': 'NUMBER', 'range': [1, 999]},
         {'id': 53, 'name': 'SELL_VOL_8', 'type': 'NUMBER', 'range': [1, 999]},
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
        if len(lines[i].strip()) == 0:
            continue

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

    sorted_columns = columns[:]
    sorted_columns.sort(key=lambda x: x['id'])

    count = 0
    values = {}
    while count < params['row_count']:
        if 'init_values' in params:
            values.update(params['init_values'])

        for column in columns:
            gen_column_value(column, values, value_params)

        value_str = ''
        for column in sorted_columns:
            # print column
            # print values
            if 'format' in column:
                value = column['format'] % values[column['name']]
            else:
                value = str(values[column['name']]) if column['type'] != 'STRING' else values[column['name']]
            value_str = value_str + '^|' + value
        value_str += '\n'
        output.write(value_str[2:])
        count += 1

        if count % 10000 == 0:
            print "%d rows." % count

    output.close()


def gen_column_value(column, values, value_params):
    def gen_string(column, values, value_params):
        # print 'generating string value for column %s' % column['name']
        if 'enum' in column:
            values[column['name']] = column['enum'][int(len(column['enum']) * np.random.random())]
        elif 'expr' in column:
            expr = column['expr']

            if column['expr'].find('{last_rand}') != -1:
                expr = expr.replace('{last_rand}', str(value_params['rand']))

            if column['expr'].find('{last_serial}') != -1:
                expr = expr.replace('{last_serial}', str(value_params['serial']))

            if column['expr'].find('{rand}') != -1:
                if 'range' in column:
                    range_section = int(len(column['range']) / 2 * np.random.random())
                    rand = column['range'][range_section * 2] + \
                           (column['range'][range_section * 2 + 1] - column['range'][range_section * 2]) \
                           * np.random.random()
                else:
                    rand = np.random.random()
                value_params['rand'] = rand
                expr = expr.replace('{rand}', str(rand))

            if column['expr'].find('{serial}') != -1:
                if 'serial' not in value_params:
                    value_params['serial'] = column['range'][0] - 1
                value_params['serial'] = value_params['serial'] + 1
                expr = expr.replace('{serial}', str(value_params['serial']))

            values[column['name']] = eval(expr, value_params, values)

    def gen_number(column, values, value_params):
        value = 0
        if 'expr' in column:
            expr = column['expr']

            if column['expr'].find('{last_rand}') != -1:
                expr = expr.replace('{last_rand}', str(value_params['rand']))

            if column['expr'].find('{last_serial}') != -1:
                expr = expr.replace('{last_serial}', str(value_params['serial']))

            if column['expr'].find('{rand}') != -1:
                if 'range' in column:
                    range_section = int(len(column['range']) / 2 * np.random.random())
                    rand = column['range'][range_section * 2] + \
                           (column['range'][range_section * 2 + 1] - column['range'][range_section * 2]) \
                           * np.random.random()
                else:
                    rand = np.random.random()

                value_params['rand'] = rand
                expr = expr.replace('{rand}', str(rand))

            if column['expr'].find('{serial}') != -1:
                if 'serial' not in value_params:
                    value_params['serial'] = column['range'][0] - 1
                value_params['serial'] = value_params['serial'] + 1
                expr = expr.replace('{serial}', str(value_params['serial']))

            value = eval(expr, value_params, values)

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

                value_params['rand'] = rand

            value = int(rand)

        values[column['name']] = value

    def gen_date(column, values, value_params):
        # print 'generating date value for column %s' % column['name']
        if 'expr' in column:
            values[column['name']] = str(eval(column['expr'], value_params, values))

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
            values[column['name']] = gen_time.tm_hour * 10000 + gen_time.tm_min * 100 + gen_time.tm_sec

    def gen_serial(column, values, value_params):
        if column['name'] not in values:
            values[column['name']] = value_params.get('INIT_SERIAL', column['range'][0])
        else:
            values[column['name']] = values[column['name']] + 1

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
        #     'row_count': 1000000,
        #     'data_date': str(cur_date),
        #     'data_file': r'c:\tmp\csdc_h_sec_tran_tst.dat',
        #     'stocks': [{'stock_id': stock[0], 'stock_name': stock[1], 'close_price': np.random.random() * 99 + 1} for
        #                stock in sh_stocks],
        # })
        gen_table_data({
            'table': 'csdc_s_sec_tran_tst',
            'row_count': 10000000,
            'data_date': str(cur_date),
            'data_file': r'd:\tmp\csdc_s_sec_tran_tst.dat',
            'stocks': [{'stock_id': stock[0], 'stock_name': stock[1], 'close_price': 1} for
                       stock in sz_stocks],
        })
        # gen_table_data({
        #     'table': 'sse_sec_idx_quot_tst',
        #     'row_count': 1348,
        #     'data_date': str(cur_date),
        #     'data_file': r'c:\tmp\sse_sec_idx_quot_tst.dat',
        #     'stocks': [{'stock_id': stock[0], 'stock_name': stock[1], 'close_price': np.random.random() * 99 + 1} for
        #                stock in sh_stocks],
        # })
        # gen_table_data({
        #     'table': 'szse_sec_td_end_quot_tst',
        #     'row_count': 1348,
        #     'data_date': str(cur_date),
        #     'data_file': r'c:\tmp\szse_sec_td_end_quot_tst.dat',
        #     'stocks': [{'stock_id': stock[0], 'stock_name': stock[1], 'close_price': np.random.random() * 99 + 1} for
        #                stock in sz_stocks],
        # })
        # gen_table_data({
        #     'table': 'sec_quot_3s_szse_s_tst',
        #     'row_count': 1000000,
        #     'data_date': str(cur_date),
        #     'data_file': r'd:\tmp\sec_quot_3s_szse_s_tst.dat',
        #     'stocks': [{'stock_id': stock[0], 'stock_name': stock[1], 'close_price': np.random.random() * 99 + 1} for
        #                stock in sz_stocks],
        # })
