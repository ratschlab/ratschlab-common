import pandas as pd
import numpy as np
from pandas.api.types import is_sparse
from scipy import sparse
import tables

'''
This module exposes two functions: to_hdf and read_hdf, which usage
is to read and write sparse Pandas DataFrames into and from hdf file.
'''


def read_hdf(fpath):
    '''
    read a hdf file into a sparse Pandas DataFrame that was previously saved with the to_hdf function of this module.

    :param fpath: location of the hdf file
    :return: an instance of Pandas DataFrame
    '''
    with tables.open_file(fpath, mode='r') as f:
        sparse_group = f.get_node("/data/sparse")
        sparse_m = _read_sparse_m_coo(sparse_group)
        col_names = f.get_node("/col_names")
        s_col_names = f.get_node("/data/sparse/sparse_col_names")
        non_sparse = pd.read_hdf(fpath, key="/data/non_sparse")
        sparse = pd.DataFrame.sparse.from_spmatrix(sparse_m, columns=s_col_names)
        ret_df = pd.concat([sparse, non_sparse], axis=1)
        ret_df = ret_df[col_names]
    return ret_df

def _read_sparse_m(group):
    attributes = []
    for attribute in ('data', 'indices', 'indptr', 'shape'):
        attributes.append(getattr(group, attribute).read())
    sparse_m = sparse.csr_matrix(tuple(attributes[:3]), shape=attributes[3])
    return sparse_m

def _read_sparse_m_coo(group):
    attributes = []
    for attribute in ('data', 'row', 'col', 'shape'):
        attributes.append(getattr(group, attribute).read())
    t = tuple([attributes[1], attributes[2]])
    sparse_m = sparse.coo_matrix(tuple([attributes[0], t]), shape=attributes[3])
    return sparse_m

def _store_sparse_m_coo(m, hdf5, group, complevel=5, complib='blosc', **kwargs):
    assert (m.__class__ == sparse.coo.coo_matrix), 'm must be a coo matrix'
    for attribute in ('data', 'row', 'col', 'shape'):
        arr = np.array(getattr(m, attribute))
        _store_array_hdf(arr, hdf5, group, attribute,
                                        complevel=complevel, complib=complib, **kwargs)
    return hdf5


def _store_sparse_m(m, hdf5, group, complevel=5, complib='blosc', **kwargs):
    assert (m.__class__ == sparse.csr.csr_matrix), 'M must be a csr matrix'
    for attribute in ('data', 'indices', 'indptr', 'shape'):
        arr = np.array(getattr(m, attribute))
        _store_array_hdf(arr, hdf5, group, attribute,
                                        complevel=complevel, complib=complib, **kwargs)
    return hdf5

def _store_array_hdf(arr, hdf5, group, name, complevel=5, complib='blosc', **kwargs):
    if arr.dtype == 'object':
        arr = arr.astype('U')
    atom = tables.Atom.from_dtype(arr.dtype)
    filters = tables.Filters(complevel=complevel, complib=complib, **kwargs)
    ds = hdf5.create_carray(group, name, atom, arr.shape, filters=filters)
    ds[:] = arr

def _store_non_sparse_df(df, path, complevel=5, complib='blosc', **kwargs):
    df.to_hdf(path, key="data/non_sparse",
                         complevel=complevel, complib=complib, **kwargs)


def to_hdf(df, path, sparse_hdf_filter_dict=None, non_sparse_hdf_filter_dict=None):
    '''
    write sparse dataframe to hdf file . The default compression level and complib are:
    complevel: 5
    complib: 'blosc'
    If you wish to change that, pass a dictionary to the function call as in the following example:
    sparse_hdf_filter_dict = {
      'complevel': 6,
      'complib': 'zlib'
    }
    non_sparse_hdf_filter_dict = {
      'complevel': 7,
      'complib': 'lzo'
    }
    to_hdf(my_df, my_path, sparse_hdf_filter_dict, non_sparse_hdf_filter_dict).

    The sparse matrix is saved in COO format
    :param df: an instance of Pandas DataFrame that you wish to save to file
    :param path: path of the
    :param sparse_hdf_filter_dict: options for hdf filters (sparse part)
    :param non_sparse_hdf_filter_dict: options for hdf filters (non sparse part)
    :return:
    '''
    kwargs_sparse = sparse_hdf_filter_dict if sparse_hdf_filter_dict is not None else {}
    kwargs_non_sparse = non_sparse_hdf_filter_dict if non_sparse_hdf_filter_dict is not None else {}
    assert isinstance(kwargs_sparse, dict) and isinstance(kwargs_non_sparse, dict)

    sparse_df, non_sparse_df = _split_sparse(df)
    sparse_m = sparse_df.sparse.to_coo()
    with tables.open_file(path, mode='w') as f:
        data_group = f.create_group("/", "data")
        data_sparse_group = f.create_group(data_group, "sparse")
        _store_array_hdf(df.columns.values, f, f.root, "col_names")
        _store_sparse_m_coo(sparse_m, f, data_sparse_group, **kwargs_sparse)
        _store_array_hdf(sparse_df.columns.values, f, data_sparse_group, "sparse_col_names")
        _store_non_sparse_df(non_sparse_df, path, **kwargs_non_sparse)


def _split_sparse(df):
    sparse_col = []
    non_sparse_col = []
    for col in df.columns:
        if is_sparse(df[col]):
            sparse_col.append(col)
        else:
            non_sparse_col.append(col)
    return df[sparse_col], df[non_sparse_col]
