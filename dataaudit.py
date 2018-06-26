import os
import six
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


def duplicate_rows(data):
    pass


def check_numeric(series):
    '''Given a string series, checks if the value is numeric or not.
    - If fully numeric, no errors
    - If it's supposed to be numeric (based on heuristics) but is not, report an error
    - If it's clearly not numeric, no errors
    '''
    pass


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
