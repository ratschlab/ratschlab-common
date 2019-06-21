
import os

import dask
import numpy as np
import pytest

from ratschlab_common.io.chunks import chunk_indexing
from ratschlab_common.io.dataframe_formats import HdfDataFrameFormat, \
    ParquetDataFrameFormat

df_formats = [HdfDataFrameFormat(), ParquetDataFrameFormat()]

@pytest.mark.parametrize("partitions", [1,3])
@pytest.mark.parametrize("dff", df_formats)
def test_chunkfile_indexing(a_df, tmpdir, dff, partitions):
    key = 'ids'

    for i in range(partitions):
        d = a_df.copy()
        d[key] = np.random.randint(i*2, (i+1)*2, len(a_df))
        dff.write_df(d, os.path.join(tmpdir, 'part-{}.{}'.format(i, dff.format_extension())))

    with dask.config.set(scheduler=dask.local.get_sync):
        index_path = chunk_indexing.index_chunkfiles(tmpdir, key, dff)

    index = chunk_indexing.load_chunkfile_index(index_path)

    assert len(index) > 0

    for k in index.keys():
        assert index[k] == 'part-{}.{}'.format(int(int(k)/2.0), dff.format_extension())


def test_chunkfile_indexing_enforce_disjoint(a_df, tmpdir):
    key = 'ids'
    dff = df_formats[0]

    for i in range(2):
        d = a_df.copy()
        d[key] = d[key] = np.random.randint(0, 2, len(a_df))
        dff.write_df(d, os.path.join(tmpdir, 'part-{}.{}'.format(i,
                                                                 dff.format_extension())))

    with dask.config.set(scheduler=dask.local.get_sync):
        with pytest.raises(ValueError):
            chunk_indexing.index_chunkfiles(tmpdir, key, dff)

