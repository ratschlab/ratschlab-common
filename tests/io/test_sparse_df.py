import pandas as pd
import numpy as np
from pandas.api.types import is_sparse
from pandas.testing import assert_frame_equal
from ratschlab_common.io.sparse_df import read_hdf, to_hdf
import random

def create_sparse_df(sparse_indexes):
    # create sparse df
    random.seed(30)
    df = pd.DataFrame(np.random.randn(10, 4))
    random_index = np.random.randint(0, 2, size=(10,))
    for i in sparse_indexes:
        df[i][random_index == 0] = 0.0
        df[i] = pd.SparseArray(df[0], fill_value=0.0)
    assert is_sparse(df[0]) == True
    return df

def test_round_trip():
    # create writer and save to disk
    df_w = create_sparse_df([0,1])
    path = "/home/lorenzo/ETH/semester3/job/ratschlab-common/tests/data/myhdf5file.h5"
    to_hdf(df_w, path)

    # create reader and read df
    df_r = read_hdf(path)

    assert_frame_equal(df_w, df_r)

def test_column_order():
    df_w = create_sparse_df([0,2])
    path = "/home/lorenzo/ETH/semester3/job/ratschlab-common/tests/data/myhdf5file.h5"
    to_hdf(df_w, path)

    # create reader and read df
    df_r = read_hdf(path)
    assert list(df_w.columns.values) == list(df_r.columns.values)

def test_options():
    df_w = create_sparse_df([0, 2])
    path = "/home/lorenzo/ETH/semester3/job/ratschlab-common/tests/data/myhdf5file.h5"
    to_hdf(df_w, path,
           sparse_hdf_filter_dict={'complevel': 6, 'complib': 'zlib'},
           non_sparse_hdf_filter_dict={'complevel': 7,'complib': 'lzo'}
           )

    # create reader and read df
    df_r = read_hdf(path)
    assert_frame_equal(df_w, df_r)


if __name__ == '__main__':
    test_round_trip()
    test_column_order()
    test_options()


