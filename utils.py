import os
import six
import csv
import xlrd
import sys
import itertools
import dateutil
import datetime
import pandas as pd
import numpy as np

def is_date(series):
    '''
    Returns ``True`` if the first 1000 non-null values in a ``series`` are
    parseable as dates
    Parameters
    ----------
    series : Pandas Series
    Examples
    --------
    Usage::
        is_date(pd.Series(['Jul 31, 2009', '2010-01-10', None]))
        # True
        is_date(pd.Series(['Jul 31, 2009', '2010-101-10', None]))
        # False
        is_date(pd.Series(pd.date_range('1/1/2011', periods=72, freq='H')))
        # True
    '''
    series = series.dropna()[:1000]
    if len(series) == 0:
        return False
    if series.apply(lambda v: issubclass(type(v), datetime.datetime)).all():
        return True
    try:
        series.apply(dateutil.parser.parse)
    except (ValueError,      # Values that cannot be converted into dates
            TypeError,       # Values that cannot be converted into dates
            AttributeError,  # Long ints do not have a .read attribute
            OverflowError):  # Long ints like mobile numbers raise this
        return False
    return True


def has_keywords(series, sep=' ', thresh=2):
    '''
    Returns ``True`` if any of the first 1000 non-null values in a string
    ``series`` are strings that have more than ``thresh`` =2 separators
    (space, by default) in them
    Parameters
    ----------
    series : pd.Series
        Must be a string series. ``series.str.count()`` should be valid.
    sep : str
        Separator within the words. Defaults to ``' '`` space.
    thresh : int
        Threshold number of times a separator should occur in the word.
        Defaults to 2.
    Examples
    --------
    Usage::
        series = pd.Series(['Curd ', 'GOOG APPL MS', 'A B C', 'T Test is'])
        has_keywords(series)
        # False
        has_keywords(series, thresh=1)
        # True
    '''
    return (series.dropna()[:1000].str.count(sep) > thresh).any()


def get_numeric_cols(data):
    numeric_data = data._get_numeric_data()
    return list(numeric_data.columns)


def types(data):
    '''
    Returns the column names in groups for the given DataFrame
    Parameters
    ----------
    data : Blaze DataFrame
    Returns
    -------
    dict : dictionary of data-types
        | groups : categorical variables that you can group by
        | dates : date parseable columns (subset of groups)
        | numbers : numerical variables that you can average
        | keywords : strings with at least two spaces
    Examples
    --------
    Consider this DataFrame::
            A   B     C           D
        0   1   2   A B C D    Jul 31, 20
        1   2   3   World is   2010-11-10
    Running ``types(data)`` returns::
        {'dates': ['D'],
         'groups': ['C', 'D'],
         'keywords': ['C'],
         'numbers': ['A', 'B']}
    '''
    typ = {}
    typ['numbers'] = get_numeric_cols(data)
    typ['groups'] = list(set(data.columns) - set(typ['numbers']))
    typ['dates'] = [col for col in data.columns if is_date(data[col])]
    typ['keywords'] = [col for col in typ['groups'] if has_keywords(data[col])]
    return typ


def read_csv(filepath):
    '''
    Read given CSV file.
    '''
    delimiter = ","
    with open(filepath) as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        delimiter = dialect.delimiter
        header_row = next(csvfile).strip('\n').split(delimiter)
    data = pd.read_csv(filepath, encoding='utf-8', sep=delimiter)
    return header_row, data


def read_xlsx(filepath):
    '''
    Read Given excel file.
    '''
    wb = xlrd.open_workbook(filepath)
    sheet = wb.sheet_by_index(0)
    header_row = sheet.row_values(0)
    data = pd.read_excel(filepath, encoding='utf-8')
    return header_row, data

def missing_values_untyped(series, meta, max=0, values=['', 'NA']):
    '''
    Reports number of missing values in a series
    '''
    strings = series[series.apply(lambda v: isinstance(v, six.string_types))]
    values = set(values)
    na = strings.apply(lambda v: v in values).sum()
    null = pd.isnull(series).sum()
    missing = null + na
    if missing > max:
        return {
            'code': 'missing_value_untyped',
            'message': '{} | {:.0f}'.format(series.name, missing),
            'series': series.name,
            'missing': missing,
            'na': na,
            'null': null,
        }


def duplicate_rows_untyped(data, meta):
    count_duplicates = data.duplicated().sum()
    if count_duplicates > 0:
        return {
            'code': 'duplicate_rows_untyped',
            'message': '{:.0f}'.format(count_duplicates),
            'duplicates': count_duplicates,
        }


def check_numeric(series):
    '''Given a string series, checks if the value is numeric or not.
    - If fully numeric, no errors
    - If it's supposed to be numeric (based on heuristics) but is not, report an error
    - If it's clearly not numeric, no errors
    '''
    pass


def count_numeric_outliers(series, low=None, high=None, max=0):
    '''Given a numerical series, counts number of outliers
    - If low is not specified, 2 percentile is taken
    - If high is not specified, 98 percentile is taken

    '''
    if series._get_numeric_data().shape[0] == 0:
        return None
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    if low is None:
        low = q1 - 1.5 * iqr
    if high is None:
        high = q3 + 1.5 * iqr
    lower_outliers = series[series < low].shape[0]
    upper_outliers = series[series > high].shape[0]
    outliers = lower_outliers + upper_outliers
    if outliers > max:
        return {
            'code': 'count_outliers_typed',
            'message': 'Outliers count in {} column(numeric): \
            {:.0f}'.format(series.name, outliers),
            'series': series.name,
            'outliers': outliers,
            'lower_outliers': lower_outliers,
            'upper_outliers': upper_outliers,
            'low': low,
            'high': high,
        }


def nulls_patterns(data):
    '''
    '''
    nulls_pattern = {}
    missing_cols = data.columns[pd.isnull(data).sum() > 0]
    for comb in range(len(missing_cols), 0, -1):
        for i in itertools.combinations(missing_cols, comb):
            cols = list(i)
            non_nulls = data[cols].dropna(how='all')
            if(non_nulls.shape[0] < data.shape[0]):
                nulls_pattern[i] = data.shape[0] - non_nulls.shape[0]
                data = data.loc[non_nulls.index]
    if len(nulls_pattern) > 1:
        return {
                'code': 'missing-patterns',
                'message': 'missing patterns found',
                'md_pattern': nulls_pattern
        }


def count_categorical_outliers(series):
    # Need to handle long tail
    if len(series._get_numeric_data()) == 1:
        return None
    series_freq = series.value_counts()
    steepest_slope = series_freq[series_freq.diff() / series_freq.shift(1) < -0.5]
    if len(steepest_slope):
        outliers = len(series_freq[series_freq <= steepest_slope.values[0]])
        message = 'Outliers count in %s column(categorical): %d' % (series.name, outliers)
        return {
            'code': 'count_categorical_outliers_typed',
            'series': series.name,
            'outliers': outliers,
            'message': message
        }


def load(path_or_file, **kwargs):
    '''
    Returns a dictionary with 2 keys:

    - ``data``: a Pandas DataFrame with the contents of the file. ``None`` if
      the file could not be loaded
    - ``error``: a list of file format or column types data errors
    '''
    pass