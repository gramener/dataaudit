import six
import csv
import xlrd
import tornado.gen
import chardet
import dateutil
import datetime
import itertools
import pandas as pd
from nltk.metrics import edit_distance
from itertools import combinations

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
    header_row = []
    encoding_str = 'utf8'
    rawdata = open(filepath, 'rb').read()
    encoding_dict = chardet.detect(rawdata)
    if encoding_dict['confidence'] > 0.8:
        encoding_str = encoding_dict.get('encoding', encoding_str)
    with open(filepath) as csvfile:
        try:
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
            csvfile.seek(0)
            delimiter = dialect.delimiter
            header_row = next(csvfile).strip('\n').split(delimiter)
        except Exception as ex:
            print(ex)
    data = pd.read_csv(filepath, encoding=encoding_str, sep=delimiter)
    return header_row, data


def read_xlsx(filepath, meta):
    '''
    Read given excel file.
    We need to identify the encoding format currently we are using `utf-8`
    '''
    sheet_index = 0
    sheetname = meta['sheetname']
    wb = xlrd.open_workbook(filepath)
    if sheetname:
        sheet_index = wb.sheet_names().index(sheetname)
    sheet = wb.sheet_by_index(sheet_index)
    header_row = sheet.row_values(0)
    data = pd.read_excel(filepath, sheet_name=sheet_index, encoding='utf-8')
    return header_row, data


@tornado.gen.coroutine
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
            'message': "In column `{}` there are {:.0f} missing values".format(series.name, missing),
            'series': series.name,
            'missing': missing,
            'na': na,
            'null': null,
        }


@tornado.gen.coroutine
def duplicate_rows_untyped(data, meta):
    count_duplicates = data.duplicated().sum()
    if count_duplicates > 0:
        return {
            'code': 'duplicate_rows_untyped',
            'message': 'File contains {:.0f} duplicate row(s)'.format(count_duplicates),
            'duplicates': count_duplicates,
        }

@tornado.gen.coroutine
def duplicate_columns_untyped(data, meta):
    '''
    To check duplicate data within multiple columns which is having
    different column name or same column name.'''
    duplicate_columns = duplicate_datacolumns(data)
    count_duplicates = len(duplicate_columns)
    if count_duplicates:
        return {
            'code': 'duplicate_columns_untyped',
            'message': 'In file {:.0f} set of columns {} have duplicate data'.format(
                count_duplicates, ','.join(['({},{})'.format(
                    sub_lst[0], sub_lst[1]) for sub_lst in duplicate_columns]),
                ),
            'duplicates': count_duplicates,
        }


def check_numeric(series):
    '''Given a string series, checks if the value is numeric or not.
    - If fully numeric, no errors
    - If it's supposed to be numeric (based on heuristics) but is not, report an error
    - If it's clearly not numeric, no errors
    '''
    pass

@tornado.gen.coroutine
def count_numeric_outliers(series, meta, low=None, high=None, max=0):
    '''Given a numerical series, counts number of outliers
    - If low is not specified, 2 percentile is taken
    - If high is not specified, 98 percentile is taken
    refer below link
    https://www.miniwebtool.com/outlier-calculator/
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
            'message': 'Column `{}` has {:.0f} numeric outlier'.format(series.name, outliers),
            'series': series.name,
            'outliers': outliers,
            'lower_outliers': lower_outliers,
            'upper_outliers': upper_outliers,
            'low': low,
            'high': high,
        }

@tornado.gen.coroutine
def nulls_patterns(data, meta):
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

@tornado.gen.coroutine
def count_categorical_outliers(series, meta):
    # Need to handle long tail
    if len(series._get_numeric_data()) == 1:
        return None
    series_freq = series.value_counts()
    steepest_slope = series_freq[series_freq.diff() / series_freq.shift(1) < -0.5]
    if len(steepest_slope):
        outliers = len(series_freq[series_freq <= steepest_slope.values[0]])
        message = 'Column `%s` has %d categorical outliers' % (series.name, outliers)
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

@tornado.gen.coroutine
def duplicate_columns_name(data, meta):
    '''
    Function to count duplicate columns names.
    '''
    header_row = meta['header']
    duplicates = list(set(x for x in header_row if header_row.count(x) > 1))
    count_duplicates = len(duplicates)
    duplicates = ['`{}`'.format(x) for x in duplicates]
    if count_duplicates > 0:
        return {
            'code': 'duplicate_columns_name',
            'message': '{:.0f} duplicate column(s) - {}'.format(
                count_duplicates, ','.join(duplicates)),
            'duplicates': count_duplicates,
        }

@tornado.gen.coroutine
def check_order_id_continuous(data, meta):
    '''
    Given a dataframe identify continuous order id.
    '''
    order_id_continuous_columns = []
    continous_threshold = 90
    for column in meta['types']['numbers']:
        s_data = data[column]
        if not (s_data.isnull().values.any()):
            s_data_diff = s_data.diff().reset_index(drop=True)
            diff_lst = (s_data_diff.dropna().unique().tolist())
            if len(diff_lst) > 1:
                diff_val_series = s_data_diff.value_counts()/len(s_data_diff.dropna().index)*100
                if diff_val_series.tolist()[0] > continous_threshold:
                    order_id_continuous_columns.append(column)
    order_columns_len = len(order_id_continuous_columns)
    if order_columns_len > 0:
       return {
           'code': 'check_order_id_continuous',
           'message': 'Column `{}` has {} percent continuity'.format(
               ','.join(order_id_continuous_columns), continous_threshold),
           'order_id_continuous': order_id_continuous_columns,
       }

def duplicate_datacolumns(data):
    '''
    Given a dataframe identify same columns based on dtype.
    Input: dataframe
         देश              city  product   sales  growth product.1  sales.1 growth.1
        भारत         Hyderabad  Biscuit   866.1  -27.0%   Biscuit    866.1   -27.0%
        भारत         Hyderabad  芯芯片片    26.4  -24.2%   芯芯片片     26.4   -24.2%
        भारत         Hyderabad    Crème    38.3  -29.1%     Crème     38.3   -29.1%
        भारत         Hyderabad     Eggs   513.7  -11.3%      Eggs    513.7   -11.3%
        भारत         Bangalore  Biscuit    41.9  -40.2%   Biscuit     41.9   -40.2%
        भारत         Bangalore  芯芯片片    52.2    6.4%   芯芯片片     52.2     6.4%

    In the above dataframe product, sales, growth column data is repeated.
    Return Duplicate column names as output
    Output:
        dups = ['sales', 'product', 'growth']
    '''
    dups = []
    for i, ac in enumerate(data):
        for j, bc in enumerate(data):
            if i >= j:
                continue
            if data[ac].equals(data[bc]):
                dups.append([ac, bc])
    return dups

@tornado.gen.coroutine
def check_primary_key_unique(data, meta):
    '''
    Given dataframe check primekey unique.
    '''
    primary_key_unique_columns = ['`{}`'.format(c) for c in data if data[c].is_unique]
    primary_columns_len = len(primary_key_unique_columns)

    if primary_columns_len > 0:
        return {
            'code': 'check_primary_key_unique',
            'message': '{:.0f} column(s) can be considered as primary key(s) {}'.format(
                primary_columns_len, ','.join(primary_key_unique_columns)),
            'primary_key_unique_columns': primary_key_unique_columns,
        }

@tornado.gen.coroutine
def check_char_len(series, meta, max=50):
    '''Check character length for non numeric columns.'''
    if series.name in meta['types']['numbers']:
        return
    row_numbers = []
    row_numbers = list(series[series.str.len() > max].index)
    if len(row_numbers) > 0:
        return {
            'code': 'Character length exceeding 50',
            'message': 'In Column `{}` rows {} have exceeded the maximum character length of {}'.format(
                series.name, ','.join([
                    '{}'.format(x) for x in row_numbers]), max),
            'series': series.name
        }


def check_prefix_expression(data, meta):
    '''
    Given dataframe check prefix for number columns.
    '''
    '''
    # Commenting to pass flake8
    for column in data.select_dtypes(exclude=['int', 'int64', 'float64', 'bool']):
        s_data = data[column]
        ext_values = s_data.str.extract(r"^\D-{0,1}\d+\.{0,1}\d+$")
        # print(ext_values)
        # print(column, ext_values[0].values.tolist())
    '''


def check_func(func, v):
    try:
        func(v)
        return True
    except ValueError:
        return False

@tornado.gen.coroutine
def check_valid_dates(series, meta, thresh=0.7):
    return None
    if series.name not in meta['types']['groups']:
        return None
    uniq = pd.Series(series.unique())
    is_valid_dates = uniq.apply(lambda v: check_func(dateutil.parser.parse, v))
    valid_dates = uniq[is_valid_dates[is_valid_dates].index]
    rows_valid = series[series.isin(valid_dates)]
    perc_rows_valid = rows_valid.shape[0] / series.shape[0]
    mess = '{}(dates) | {:.0f}% values are valid dates'.format(
        series.name, perc_rows_valid*100)
    if perc_rows_valid > thresh:
        return {
            'code': 'identify_valid_dates',
            'message': mess,
            'series': series.name,
        }

@tornado.gen.coroutine
def check_negative_numbers(series, meta, thresh=0.02):
    '''
    Function to check if there is any small percentage of negative numbers
    '''
    if series.name not in meta['types']['numbers']:
        return None
    neg_nums_count = (series < 0).sum()
    perc_negs = neg_nums_count / series.shape[0]
    if perc_negs < thresh and neg_nums_count != 0:
        return {
            'code': 'negative_values_typed',
            'message': '{} | {:.0f} values are negative'.format(series.name, neg_nums_count),
            'series': series.name
        }

@tornado.gen.coroutine
def check_groups_typos(series, meta, thresh=0.02, max_dis=3):
    groups = meta['types']['groups']
    exclusion = meta['types']['dates']
    exclusion.extend(meta['types']['keywords'])
    print("*" * 100)
    if series.name not in groups or series.name in exclusion:
        return None
    freqs = series.value_counts()
    freqs = freqs[(pd.Series(freqs.index).str.len() > 5).values]
    if freqs.shape[0] == 0:
        return None
    typos = []
    print(combinations(freqs.index))
    for w1, w2 in combinations(freqs.index, r=2):
        ed = edit_distance(w1, w2)
        if ed < max_dis:
            typos.append((w1, w2))
    if len(typos):
        return {
            'code': 'typo_values_typed',
            'message': '{} | {:.0f} typos present'.format(series.name, len(typos)),
            'series': series.name,
            'typos': typos
        }