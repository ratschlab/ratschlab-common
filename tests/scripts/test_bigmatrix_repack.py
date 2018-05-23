from ratschlab_common.scripts import bigmatrix_repack as br
from ratschlab_common.io import bigmatrix
from ratschlab_common.io.bigmatrix import TablesBigMatrixWriter
import pytest
import numpy as np
from click.testing import CliRunner
from pathlib import Path
import pandas as pd
import tables
import shutil

@pytest.fixture
def simple_bigmatrix_file(tmpdir):
    rows = 10
    cols = 10
    matrix = np.random.random(size=(rows, cols))
    col_desc = pd.DataFrame.from_dict({'name': np.arange(0, cols)})
    row_desc = pd.DataFrame.from_dict({'name': np.arange(0, rows)})

    src_path = Path(tmpdir, 'src.h5')
    TablesBigMatrixWriter().write(src_path, matrix, row_desc, col_desc)

    return src_path


def test_bigmatrix_repack_base_usage(simple_bigmatrix_file, tmpdir):
    dest_path = Path(tmpdir, 'dest.h5')

    chunkshape = (3, 3)

    runner = CliRunner()
    result = runner.invoke(br.main, [str(simple_bigmatrix_file),
                                      str(dest_path),
                            '--matrix-chunkshape', str(chunkshape)])

    assert result.exit_code == 0

    with tables.open_file(str(dest_path), 'r') as hf:
        mat = hf.get_node("/{}".format(bigmatrix.DATA_KEY))
        assert mat.chunkshape == chunkshape
        assert mat.filters == TablesBigMatrixWriter.BLOSC_FILTER

        assert hf.get_node("/{}/table".format(
            bigmatrix.ROW_DESC_KEY)).filters.complib == \
               TablesBigMatrixWriter.BLOSC_FILTER.complib

        assert hf.get_node("/{}/table".format(
            bigmatrix.COL_DESC_KEY)).filters.complib == \
               TablesBigMatrixWriter.BLOSC_FILTER.complib


def test_bigmatrix_force(simple_bigmatrix_file, tmpdir):
    dest_path = Path(tmpdir, 'dest.h5')

    shutil.copy(simple_bigmatrix_file, dest_path)

    runner = CliRunner()
    result = runner.invoke(br.main, [str(simple_bigmatrix_file),
                                     str(dest_path),
                                     '--matrix-chunkshape', 'auto'])

    assert result.exit_code == -1

    result = runner.invoke(br.main, [str(simple_bigmatrix_file),
                                     str(dest_path),
                                     '--matrix-chunkshape', 'auto', '--force'])

    assert result.exit_code == 0
