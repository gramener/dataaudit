import os
from dataaudit import load

folder = os.path.dirname(os.path.abspath(__file__))
repo = os.path.normpath(os.path.join(folder, '..', 'testdata'))


def test_pdf():
    result = load(os.path.join(repo, 'data.pdf'))
    assert result['error'][0]['code'] == 'format'
    assert 'PDF' in result['error'][0]['message']


def test_csv():
    result = load(os.path.join(repo, 'sales.csv'))
    assert len(result['error']) == 0


def test_xlsx():
    result = load(os.path.join(repo, 'sales.xlsx'))
    assert len(result['xlsx']) == 0
