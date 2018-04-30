import pytest
import os
import pandas.testing

from ratschlab_common.io.dataframe_formats import HdfDataFrameFormat, ParquetDataFrameFormat


@pytest.mark.parametrize("dff", [
    HdfDataFrameFormat(),
    ParquetDataFrameFormat()
])
def test_write_df(a_df, dff, tmpdir):
    f = os.path.join(tmpdir, 'test_file.{}'.format(dff.format_extension()))

    dff.write_df(a_df, f)

    mydf_back = dff.read_df(f)
    pandas.testing.assert_frame_equal(a_df, mydf_back)

@pytest.mark.parametrize("dff", [
    HdfDataFrameFormat(),
    ParquetDataFrameFormat()
])
def test_write_df_compression(a_df, tmpdir, dff):
    f_compr = os.path.join(tmpdir, 'test_file_compr.{}'.format(dff.format_extension()))
    f = os.path.join(tmpdir, 'test_file.{}'.format(dff.format_extension()))

    dff.write_df(a_df, f_compr, None)

    if isinstance(dff, HdfDataFrameFormat):
        options = {'complevel':0}
    elif isinstance(dff, ParquetDataFrameFormat):
        options = {'compression': None}
    else:
        raise TypeError("Don't know how to handle type {}.".format(type(dff)))

    dff.write_df(a_df, f, options)

    compr_size = os.path.getsize(f_compr)
    uncompr_size = os.path.getsize(f)

    assert compr_size < uncompr_size

