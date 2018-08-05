
import os
import six
import sys
import itertools
import pandas as pd


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
    pass


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


def nulls_patterns(data):
    '''
    '''
    nulls_pattern = {}
    missing_cols = data.columns[pd.isnull(data).sum()>0]
    for comb in range(len(missing_cols), 0, -1):
        for i in itertools.combinations(missing_cols, comb):
            cols = list(i)
            non_nulls = data[cols].dropna(how='all')
            if(non_nulls.shape[0] < data.shape[0]):
                nulls_pattern[i] = data.shape[0] - non_nulls.shape[0]
                data = data.loc[non_nulls.index]
    if len(nulls_pattern)>1:
        return {
                'code': 'missing-patterns',
                'message': 'missing patterns found',
                'md_pattern': nulls_pattern
        }


def count_categorical_outliers(series):
    # Need to handle long tail
    series_freq = series.value_counts()
    steepest_slope = series_freq[series_freq.diff() / series_freq.shift(1) < -0.5]
    if len(steepest_slope):
        outliers = len(series_freq[series_freq <= steepest_slope.values[0]])
        return {
            'code': 'count_categorical_outliers_typed',
            'series': series.name,
            'outliers': outliers,
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
registry['column-typed'].append(count_outliers_typed)
registry['data-untyped'].append(nulls_patterns)

def print_test(test_output):
    if test_output != None:
        print(test_output['message'])


def run_audits(path):
    # Check this function with Anand
    # How should we print results
    # Hence not optimizing
    try:
        print(path)
        data = pd.read_csv(path)
        print ('========Data Untyped==============')
        for method in registry['data-untyped']:
            test_data = method(data)
            print_test(test_data)
        print ('========Column Untyped============')
        for method in registry['column-untyped']:
            for col in data.columns:
                col_test = method(data[col])
                print_test(col_test)
        print ('=======Column Typed=================')
        for method in registry['column-typed']:
            for col in data.columns:
                col_test = method(data[col])
                print_test(col_test)
    except (FileNotFoundError, pd.errors.ParserError) as e:
        print (e)
        print('Problem loading the data. Pass the correct CSV file wit correct path')
    return 'Audits sucessfully done'
if __name__ == "__main__":
    
    args = sys.argv
    if len(args) > 1:
        path = sys.argv[1]
        run_audits(path)
    else:
        print('No file path passed. Pass one')