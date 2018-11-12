import os
import six
import sys
import utils
import pandas as pd


def is_a_file(source, meta={}):
    errors = {}
    # First need to check if it is a file
    filename, file_extension = os.path.splitext(source)
    meta['file_extension'] = file_extension
    if file_extension in ['.csv', '.tsv']:
        header, data = utils.read_csv(source)
    elif file_extension == '.xlsx':
        header, data = utils.read_xlsx(source, meta)
    meta['header'] = header
    meta['types'] = utils.types(data)
    return data, meta, errors


def is_a_database(source):
    return False


def check(source, **kwargs):
    '''
    Returns a list of data quality errors
    '''
    errors = []
    meta = {}
    meta['sheetname'] = kwargs['sheetname']
    # Load the data
    if isinstance(source, six.text_type):
        # if is_a_file(source, meta):
        data, meta, error = is_a_file(source, meta)
        # elif is_a_database(source):
            # data = pd.read_sql(source, **kwargs)
    elif isinstance(source, pd.DataFrame):
        data = source
    else:
        raise RuntimeError('source must be file, DB URL or DataFrame, not %s' % source)

    # Apply type checks and conversations
    for method in registry['data-untyped']:
        report(errors, method(data, meta), meta)
    for method in registry['column-untyped']:
        for col in data.columns:
            report(errors, method(data[col], meta), meta)

    # Check typed data
    for method in registry['data-typed']:
        report(errors, method(data, meta), meta)
    for method in registry['column-typed']:
        for col in data.columns:
            report(errors, method(data[col], meta), meta)
    return errors


def report(errors, result, meta, **kwargs):
    if result is None:
        return
    result.update(kwargs)
    errors.append(result)
    return errors


registry = {
    'file': [],             # List of file-level checks
                            # <STRUCTURE INFERENCE> happens here
    'data-untyped': [],     # List of data-level untyped checks
    'column-untyped': [],   # List of column-level untyped checks
                            # <TYPE INFERENCE> happens here
    'data-typed': [],
    'column-typed': [],
}


registry['column-untyped'].extend([
    utils.missing_values_untyped,
    utils.check_char_len])
registry['data-untyped'].extend([
    utils.duplicate_rows_untyped,
    utils.duplicate_columns_name,
    utils.duplicate_columns_untyped,
    utils.nulls_patterns,
    utils.check_order_id_continuous,
    utils.check_primary_key_unique])
registry['column-typed'].extend([
    utils.count_numeric_outliers,
    utils.count_categorical_outliers,
    utils.check_valid_dates,
    utils.check_negative_numbers,
    utils.check_groups_typos])

if __name__ == "__main__":
    args = sys.argv
    sheetname = ''
    if len(args) > 1:
        path = sys.argv[1]
        if len(sys.argv) >= 3:
            sheetname = sys.argv[2]
        errors = check(path, sheetname=sheetname)
        for error in errors:
            print(error['code'], ' | ', error['message'])
    else:
        print('No file path passed. Pass one')
