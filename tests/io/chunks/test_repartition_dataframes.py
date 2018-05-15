from pathlib import Path

import dask.dataframe
import numpy as np
import pandas as pd
import pytest

from ratschlab_common.collections import set_utils
from ratschlab_common.io.chunks import repartition_dataframes
from ratschlab_common.io.dataframe_formats import HdfDataFrameFormat, \
    ParquetDataFrameFormat

DEFAULT_KEY='ids'

df_formats = [HdfDataFrameFormat(), ParquetDataFrameFormat()]

@pytest.mark.parametrize("ids,npartitions,expected", [
    ([1,2,3], 1, [1,3]),
    ([1,2,3,4], 2, [1,3,4]),
    ([1,1,1,1,1], 2, [1]),
    ([1,1,1,1,2], 2, [1,2]),
    (["A","B","C","D","E"], 2, ["A","D", "E"]),
    ([1,2,3,4,5], 2, [1,4,5]),
    ([1,2,3,4,5,6], 2, [1,4,6]),
    ([1,2,3,4,5,6,7,8], 3, [1,4,7,8])
])
def test_compute_divisions(ids, npartitions, expected):
    np.testing.assert_array_equal(repartition_dataframes._compute_divisions(ids, npartitions),np.array(expected))

# TODO: raise error on     ([], 1, []),

# TODO: test with uniform ids!

@pytest.mark.parametrize("start_partitions,end_partitions", [
    (1, 1),
    (2, 4),
    (2, 7),
    (4, 2)
])
def test_basic_repartition_dataframes(start_partitions, end_partitions):
    ddf = dask.dataframe.from_pandas(_create_simple_df(), npartitions=start_partitions)

    assert start_partitions == ddf.npartitions

    ddf_rep = repartition_dataframes.repartition_dask_dataframe(ddf, end_partitions, DEFAULT_KEY)

    # depending on the data and the partitioning scheme, we may end up with fewer partitions
    part_diff = end_partitions - ddf_rep.npartitions
    assert  part_diff == 0 or part_diff == 1

    assert _has_disjoint_keys(ddf_rep, DEFAULT_KEY)

@pytest.mark.parametrize("start_partitions,end_partitions", [
    (1, 1),
    (2, 4),
    (4, 2)
])
@pytest.mark.parametrize("df_format", df_formats)
@pytest.mark.parametrize("key", [DEFAULT_KEY, None])
def test_repartition_directory(tmpdir, start_partitions, end_partitions, df_format, key):
    df = _create_simple_df()

    ddf = dask.dataframe.from_pandas(df, npartitions=start_partitions)

    orig_path = Path(tmpdir, 'orig')
    orig_path.mkdir(exist_ok=True)

    df_format.write_dask_df(ddf, orig_path).compute()

    dest_path = Path(tmpdir, 'dest')
    g = repartition_dataframes.repartition_directory(orig_path, dest_path,
                                                     end_partitions, key, df_format)
    g.compute()

    ddf_back = df_format.read_dask_df(dest_path)
    assert end_partitions == ddf_back.npartitions # 'nice' partitioning, so this should hold

    if key:
        assert _has_disjoint_keys(ddf_back, key)


def _has_disjoint_keys(ddf, key):
    keys_per_partition = [ddf.get_partition(i)[key].unique().compute()
                          for i in range(ddf.npartitions)]
    return set_utils.are_pairwise_disjoint(keys_per_partition)

# TODO: refactor?
def _create_simple_df(records=100, n_cols=2, key=DEFAULT_KEY):
    ids = np.random.randint(0, records / 5, records)
    # TODO: refactor: take some dataframe, append column
    cols = {key: ids}
    for c in range(n_cols):
        cols['col{}'.format(c)] = np.random.rand(records)

    df = pd.DataFrame(cols)
    return df
