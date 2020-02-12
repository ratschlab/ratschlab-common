import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
from ratschlab_common.io.sparse_df import read_hdf, to_hdf
import random
import pytest


@pytest.fixture(scope='module', params=[([0,1], None), ([0,2], None), ([0,1],['a', 'b', 'c', 'd'])])
def sparse_df(request):
    # create sparse df
    random.seed(30)
    df = pd.DataFrame(np.random.randn(10, 4), columns=request.param[1])
    random_index = np.random.randint(0, 2, size=(10,))
    for i in request.param[0]:
        df[df.columns[i]][random_index == 0] = 0.0
        df[df.columns[i]] = pd.SparseArray(df[df.columns[i]], fill_value=0.0)
    return df

def test_round_trip(sparse_df, tmp_path):
    # create writer and save to disk
    df_w = sparse_df
    path = tmp_path / "myhdf5file.h5"
    to_hdf(df_w, path)

    # create reader and read df
    df_r = read_hdf(path)

    assert_frame_equal(df_w, df_r)

def test_column_order(sparse_df, tmp_path):
    df_w = sparse_df
    path = tmp_path / "myhdf5file.h5"
    to_hdf(df_w, path)

    # create reader and read df
    df_r = read_hdf(path)
    assert list(df_w.columns.values) == list(df_r.columns.values)

def test_options(sparse_df, tmp_path):
    df_w = sparse_df
    path = tmp_path / "myhdf5file.h5"
    to_hdf(df_w, path,
           sparse_hdf_filter_dict={'complevel': 6, 'complib': 'zlib'},
           non_sparse_hdf_filter_dict={'complevel': 7,'complib': 'lzo'}
           )

    # create reader and read df
    df_r = read_hdf(path)
    assert_frame_equal(df_w, df_r)
