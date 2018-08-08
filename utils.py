import dateutil
import datetime
import pandas as pd

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
    return numeric_data.columns


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


def check_prefix_expression(data):
    '''
    Given dataframe check prefix for number columns.
    '''
    for column in data.select_dtypes(exclude=['int', 'int64', 'float64', 'bool']):
        s_data = data[column]
        ext_values = s_data.str.extract(r"^\D-{0,1}\d+\.{0,1}\d+$")
        print(ext_values)
        # print(column, ext_values[0].values.tolist())
