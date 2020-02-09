import pandas as pd
import numpy as np
from pandas.api.types import is_sparse
from scipy import sparse
import tables

# need to check which sparse matrix to use (for now supports only csr_matrix)


def read_hdf(fpath):
    '''
    read a hdf file into a sparse Pandas DataFrame that was previously saved with the to_hdf function of this module.

    :param fpath: location of the hdf file
    :return: an instance of Pandas DataFrame
    '''
    f = tables.open_file(fpath, mode='r')
    sparse_group = f.get_node("/data/sparse")
    sparse_m = _read_sparse_m(sparse_group)
    s_col_names = f.get_node("/data/sparse/col_names")
    non_sparse = pd.read_hdf(fpath, key="/data/non_sparse")
    sparse = pd.DataFrame.sparse.from_spmatrix(sparse_m, columns=s_col_names)
    ret_df = pd.concat([sparse, non_sparse], axis=1)
    f.close()
    return ret_df

def _read_sparse_m(group):
    attributes = []
    for attribute in ('data', 'indices', 'indptr', 'shape'):
        attributes.append(getattr(group, attribute).read())
    sparse_m = sparse.csr_matrix(tuple(attributes[:3]), shape=attributes[3])
    return sparse_m


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


def to_hdf(df, path, **kwargs):
    '''
    write sparse dataframe to hdf file. The default compression level and complib are:
    complevel: 5
    complib: 'blosc'
    If you wish to change that, pass a dictionary to the function call as in the following example:
    pars = {
     'sparse': {
      'complevel': 6,
      'complib': 'zlib'
     }
     'non_sparse': {
      'complevel': 7,
      'complib': 'lzo'
     }
    }
    to_hdf(my_df, my_path, **pars)
    :param df: an instance of Pandas DataFrame that you wish to save to file
    :param path: path of the
    :param kwargs:
    :return:
    '''
    kwargs_sparse = kwargs.get('sparse', {})
    kwargs_non_sparse = kwargs.get('non_sparse', {})
    assert isinstance(kwargs_sparse, dict) and isinstance(kwargs_non_sparse, dict)

    sparse_df, non_sparse_df = _split_sparse(df)
    sparse_m = sparse.csr_matrix(sparse_df.values)
    f = tables.open_file(path, mode='w')
    data_group = f.create_group("/", "data")
    data_sparse_group = f.create_group(data_group, "sparse")
    _store_sparse_m(sparse_m, f, data_sparse_group, **kwargs_sparse)
    _store_array_hdf(sparse_df.columns.values, f, data_sparse_group, "col_names")
    _store_non_sparse_df(non_sparse_df, path, **kwargs_non_sparse)
    f.close()


def _split_sparse(df):
    sparse_col = []
    non_sparse_col = []
    for col in df.columns:
        if is_sparse(df[col]):
            sparse_col.append(col)
        else:
            non_sparse_col.append(col)
    return df[sparse_col], df[non_sparse_col]




if __name__ == '__main__':
    # create sparse df
    sparse_indexes = [0, 1]
    df = pd.DataFrame(np.random.randn(10, 4))
    random_index = np.random.randint(0, 2, size=(10, ))
    for i in sparse_indexes:
        df[i][random_index == 0] = 0.0
        df[i] = pd.SparseArray(df[0], fill_value=0.0)
    print(is_sparse(df[0]))
    print(df.head())
    print(df.dtypes)

    # create writer and save to disk
    path = "myhdf5file.h5"
    to_hdf(df, path)

    # create reader and read df
    read_hdf(path)
    print(df.head())
    print(df.dtypes)

    '''
    print(writer.sparse_m.data, writer.sparse_m.indices, writer.sparse_m.indptr, writer.sparse_m.shape)
    sdf = df.astype(pd.SparseDtype("float", np.nan))
    print(sdf.sparse.density)
    '''
