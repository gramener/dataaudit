import os
import pandas as pd
import dataaudit
import logging

folder = os.path.dirname(os.path.abspath(__file__))
repo = os.path.normpath(os.path.join(folder,  'testdata'))


def test_missing_values():    
    errors = dataaudit.check(os.path.join(repo, 'sales.csv'), sheetname="")
    
    assert errors[0]['code'] == 'duplicate_columns_name'
    assert 'sales' in errors[0]['message']
    assert 'growth' in errors[0]['message']

    assert errors[1]['code'] == 'duplicate_columns_untyped'
    assert 'sales' in errors[1]['message']

    assert errors[3]['code'] == 'check_order_id_continuous'
    assert 'order_id' in errors[3]['message']

    assert errors[4]['code'] == 'check_primary_key_unique'
    assert 'order_id' in errors[4]['message']

    assert errors[5]['code'] == 'missing_value_untyped'
    assert 'sales' in errors[0]['message']


# def test_mutiple_columns():
#     filename = os.path.join(repo, 'sales.csv')
#     log_file = os.path.join(repo, 'log.log')
#     result = dataaudit.load(os.path.join(repo, 'sales.csv'),  file_extension='csv')
#     header_row = result.get('header_row', [])
#     data = result.get('data', [])
#     logger = logging.getLogger()
#     logger.setLevel(logging.DEBUG)
#     logging.basicConfig(filename=log_file, filemode='w')
#     logging.info(dataaudit.duplicate_columns_name(header_row))
#     logging.info(dataaudit.duplicate_columns_untyped(data))
#     logging.info(dataaudit.check_order_id_continous(data))
#     logging.info(dataaudit.check_primary_key_unique(data))
#     result = dataaudit.load(os.path.join(repo, 'sales.xlsx'),  file_extension='excel')
#     header_row = result.get('header_row', [])
#     data = result.get('data', [])
#     logging.info(dataaudit.duplicate_columns_name(header_row))
#     logging.info(dataaudit.duplicate_columns_untyped(data))

# test_missing_values()