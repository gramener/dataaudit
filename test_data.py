import os
import pandas as pd
import dataaudit
# from dataaudit import load, check

folder = os.path.dirname(os.path.abspath(__file__))
repo = os.path.normpath(os.path.join(folder,  'testdata'))
# repo = os.path.normpath(os.path.join(folder, '..', 'testdata'))


def test_missing_values():
    result = dataaudit.load(os.path.join(repo, 'sales.csv'))
    errors = dataaudit.check(result['data'])

    assert errors[0]['code'] == 'missing-value'
    assert 'sales' in errors[0]['message']

    assert errors[1]['code'] == 'missing-value'
    assert 'growth' in errors[1]['message']


def test_mutiple_columns():
    filename = os.path.join(repo, 'sales.csv')
    header_row, data = dataaudit.load_data(filename, 'csv')
    print(dataaudit.duplicate_columns_name(header_row))
    print(dataaudit.duplicate_columns_untyped(data))
    filename = os.path.join(repo, 'sales.xlsx')
    header_row, data = dataaudit.load_data(filename, 'excel')
    print(dataaudit.duplicate_columns_name(header_row))
    print(dataaudit.duplicate_columns_untyped(data))

test_mutiple_columns()
