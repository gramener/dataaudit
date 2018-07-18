import os
import six
import csv
import xlrd
import pandas as pd
import numpy as np


def check(source, **kwargs):
    '''
    Returns a list of data quality errors
    '''
    errors = []
    # Load the data
    if isinstance(source, six.text_type):
        if is_a_file(source):
            data, errors = check_file(source)
        elif is_a_database(source):
            data = pd.read_sql(source, **kwargs)
    elif isinstance(source, pd.DataFrame):
        data = source
    else:
        raise RuntimeError('source must be file, DB URL or DataFrame, not %s' % source)

    # Apply type checks and conversations
    for method in registry['data-untyped']:
        report(errors, method(data))
    for method in registry['column-untyped']:
        for col in data.columns:
            report(errors, method(data[col]))

    # Check typed data
    for method in registry['data-typed']:
        report(errors, method(data))
    for method in registry['column-typed']:
        for col in data.columns:
            report(errors, method(data[col]), column=col)

    return errors


def report(errors, result, **kwargs):
    if result is None:
        return
    result.update(kwargs)
    errors.append(result)


def load(path_or_file, **kwargs):
    '''
    Returns a dictionary with 2 keys:

    - ``data``: a Pandas DataFrame with the contents of the file. ``None`` if
      the file could not be loaded
    - ``error``: a list of file format or column types data errors
    '''
    # pass


def missing_values_untyped(series, max=0, values=['', 'NA']):
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
            'message': '{}: {:.0f} values missing'.format(series.name, missing),
            'series': series.name,
            'missing': missing,
            'na': na,
            'null': null,
        }


def duplicate_rows_untyped(data):
    count_duplicates = data.duplicated().sum()
    if count_duplicates > 0:
        return {
            'code': 'duplicate_rows_untyped',
            'message': '{:.0f} duplicate rows'.format(count_duplicates),
            'duplicates': count_duplicates,
        }


def duplicate_columns_name(header_row):
    '''
    Function to count duplicate columns names.'''
    count_duplicates = len(duplicate_column_headers(header_row))
    if count_duplicates > 0:
        return {
            'code': 'duplicate_column_headers',
            'message': '{:.0f} duplicate columns'.format(count_duplicates),
            'duplicates': count_duplicates,
        }


def duplicate_columns_untyped(data):
    '''
    To check duplicate data within multiple columns which is having
    different column name or same column name.'''
    count_duplicates = len(duplicate_datacolumns(data))
    if count_duplicates > 0:
        return {
            'code': 'duplicate_columns_untyped',
            'message': '{:.0f} duplicate columns'.format(count_duplicates),
            'duplicates': count_duplicates,
        }


def check_numeric(series):
    '''Given a string series, checks if the value is numeric or not.
    - If fully numeric, no errors
    - If it's supposed to be numeric (based on heuristics) but is not, report an error
    - If it's clearly not numeric, no errors
    '''
    pass


def count_outliers_typed(series, low=None, high=None, max=0):
    '''Given a numerical series, counts number of outliers
    - If low is not specified, 2 percentile is taken
    - If high is not specified, 98 percentile is taken

    '''
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
            'message': '{}: {:.0f} outlier values'.format(series.name, outliers),
            'series': series.name,
            'outliers': outliers,
            'lower_outliers': lower_outliers,
            'upper_outliers': upper_outliers,
            'low': low,
            'high': high,
        }


def count_categorical_outliers_typed(series):
    '''
    Given a numerical series, counts number of outliers
    '''
    freqs = series.value_counts()
    return count_outliers_typed(freqs, high=freqs.max())


def duplicate_column_headers(header_row):
    '''
    Read given file based on file_extension and return duplicate count.
    '''
    header_dict = {i: header_row.count(i) for i in header_row}
    duplic_arr = [key for key, value in header_dict.items() if value > 1]
    return duplic_arr


def load_data(filepath, file_extension):
    '''
    Load data from file based on file type.  
    '''
    if file_extension == 'csv':
        header_row, data = read_csv_file(filepath)
    elif file_extension == 'excel':
        header_row, data = read_excel_file(filepath)
    return header_row, data


def read_csv_file(filepath):
    '''
    Read given CSV file.
    '''
    f_data = []
    delimiter = ","
    with open(filepath) as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        # Seek to beginning.
        csvfile.seek(0)
        data_r = csv.reader(csvfile, dialect)
        delimiter = dialect.delimiter
        f_data = [i for i in data_r]
    header_row = f_data[0]
    data = pd.read_csv(filepath, encoding='utf-8', sep=delimiter)
    return header_row, data



def read_excel_file(filepath):
    '''
    Read Given excel file.
    '''
    wb = xlrd.open_workbook(filepath)
    sheet = wb.sheet_by_index(0)
    header_row = sheet.row_values(0)
    data = pd.read_excel(filepath, encoding='utf-8')
    return header_row, data


def duplicate_datacolumns(data):
    '''
    Given a data identify same columns based on dtype.
    '''
    groups = data.columns.to_series().groupby(data.dtypes).groups
    dups = []
    for t, v in groups.items():
        cs = data[v].columns
        vs = data[v]
        lcs = len(cs)
        for i in range(lcs):
            for j in range(i+1, lcs):
                fill_na_flag = False   
                if t == np.float64 or t == np.int64:
                    iv_nan= vs.iloc[:,i].index[vs.iloc[:,i].apply(np.isnan)].values.tolist()
                    jv_nan= vs.iloc[:,j].index[vs.iloc[:,j].apply(np.isnan)].values.tolist()
                    if np.array_equiv(iv_nan, jv_nan):
                        fill_na_flag = True
                if fill_na_flag:
                    iv_series = vs.iloc[:,i].fillna(0)
                    jv_series = vs.iloc[:,j].fillna(0)
                else:
                    iv_series = vs.iloc[:,i]
                    jv_series = vs.iloc[:,j]
                iv = iv_series.tolist()
                jv = jv_series.tolist()
                if np.array_equiv(iv, jv):
                    dups.append(cs[i])
                    break
    return dups


registry = {
    'file': [],             # List of file-level checks
                            # <STRUCTURE INFERENCE> happens here
    'data-untyped': [],     # List of data-level untyped checks
    'column-untyped': [],   # List of column-level untyped checks
                            # <TYPE INFERENCE> happens here
    'data-typed': [],
    'column-typed': [],
}
registry['column-untyped'].append(missing_values_untyped)
registry['data-untyped'].append(duplicate_rows_untyped)
registry['column-typed'].append(count_outliers_typed)
