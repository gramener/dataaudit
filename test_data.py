import os
from dataaudit import load, check

folder = os.path.dirname(os.path.abspath(__file__))
repo = os.path.normpath(os.path.join(folder, '..', 'testdata'))


def test_missing_values():
    result = load(os.path.join(repo, 'sales.csv'))
    errors = check(result['data'])

    assert errors[0]['code'] == 'missing-value'
    assert 'sales' in errors[0]['message']

    assert errors[1]['code'] == 'missing-value'
    assert 'growth' in errors[1]['message']
