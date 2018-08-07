
import six
import csv
import xlrd
import sys
import itertools
import pandas as pd
import numpy as np

dataaudit_dict = {}


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
    file_extension = kwargs.get('file_extension', 'csv')
    if file_extension == 'csv':
        header_row, data = read_csv(path_or_file)
    elif file_extension == 'excel':
        header_row, data = read_xlsx(path_or_file)
    return {'header_row': header_row, 'data': data}


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


def duplicate_columns_name(data):
    '''
    Function to count duplicate columns names.'''
    header_row = dataaudit_dict['csv_headers']
    count_duplicates = len(duplicate_column_headers(header_row))
    if count_duplicates > 0:
        return {
            'code': 'duplicate_column_headers',
            'message': '{:.0f} duplicate columns with same header'.format(count_duplicates),
            'duplicates': count_duplicates,
        }


def duplicate_columns_untyped(data):
    '''
    To check duplicate data within multiple columns which is having
    different column name or same column name.'''
    count_duplicates = len(duplicate_datacolumns(data))
    if count_duplicates:
        return {
            'code': 'duplicate_columns_untyped',
            'message': '{:.0f} duplicate data columns'.format(count_duplicates),
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

def duplicate_column_headers(header_row):
    '''
    Read given file based on file_extension and return duplicate count.
    '''
    return list(set(x for x in header_row if header_row.count(x) > 1))


def read_csv(filepath):
    '''
    Read given CSV file.
    '''
    f_data = []
    delimiter = ","
    with open(filepath) as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        delimiter = dialect.delimiter
        header_row = next(csvfile).strip('').split(delimiter)
    dataaudit_dict['csv_headers'] = header_row
    data = pd.read_csv(filepath, encoding='utf-8', sep=delimiter)
    return data



def read_xlsx(filepath):
    '''
    Read Given excel file.
    '''
    wb = xlrd.open_workbook(filepath)
    sheet = wb.sheet_by_index(0)
    header_row = sheet.row_values(0)
    dataaudit_dict['excel_headers'] = header_row
    data = pd.read_excel(filepath, encoding='utf-8')
    return data


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


def check_order_id_continuous(data):
    '''
    Given a dataframe identify continuous order id.
    '''
    order_id_continuous_columns = []
    for column in data._get_numeric_data():
        s_data = data[column]
        if not (s_data.isnull().values.any()):
            s_data_diff = s_data.diff().reset_index(drop=True)
            if len(s_data_diff.unique()) == 2:
                order_id_continuous_columns.append(column)
    if len(order_id_continuous_columns) > 0:
        return {
            'code': 'check_order_id_continuous',
            'message': '{:.0f} continuous columns'.format(len(order_id_continuous_columns)),
            'order_id_continuous': order_id_continuous_columns,
        }


def check_primary_key_unique(data):
    '''
    Given dataframe check primekey unique.
    '''
    primary_key_unique_columns = []
    for column in data:
        s_data = data[column]
        if not (s_data.isnull().values.any()):
            if len(s_data) == len(s_data.unique()):
                primary_key_unique_columns.append(column)
    if len(primary_key_unique_columns) > 0:
        return {
            'code': 'check_primary_key_unique',
            'message': '{:.0f} primary columns'.format(len(primary_key_unique_columns)),
            'primary_key_unique_columns': primary_key_unique_columns,
        }


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
registry['data-untyped'].append(duplicate_columns_name)
registry['data-untyped'].append(duplicate_columns_untyped)
registry['data-untyped'].append(check_order_id_continuous)
registry['data-untyped'].append(check_primary_key_unique)
# registry['column-typed'].append(count_outliers_typed)
# registry['data-untyped'].append(nulls_patterns)


def print_test(test_output):
    if test_output is not None:
        print(test_output['message'])


def run_audits(path):
    # Check this function with Anand
    # How should we print results
    # Hence not optimizing
    try:
        print(path)
        # data = pd.read_csv(path)
        data = read_csv(path)
        print ('========Data Untyped==============')
        for method in registry['data-untyped']:
            test_data = method(data)
            print_test(test_data)
        print('========Column Untyped============')
        for method in registry['column-untyped']:
            for col in data.columns:
                col_test = method(data[col])
                print_test(col_test)
        print('=======Column Typed=================')
        for method in registry['column-typed']:
            for col in data.columns:
                col_test = method(data[col])
                print_test(col_test)
    except (FileNotFoundError, pd.errors.ParserError) as e:
        print(e)
        print('Problem loading the data. Pass the correct CSV file wit correct path')
    return 'Audits sucessfully done'


if __name__ == "__main__":
    args = sys.argv
    if len(args) > 1:
        path = sys.argv[1]
        run_audits(path)
    else:
        print('No file path passed. Pass one')
