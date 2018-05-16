import datetime
from collections import namedtuple
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import tables

import ratschlab_common.io.bigmatrix as bigmatrix
from ratschlab_common.io.bigmatrix import TablesBigMatrixWriter, \
    TablesBigMatrixReader, AxisDescription, MetadataFlavor

BigMatrixElement = namedtuple('BigMatrixElement',
                              ['matrix', 'row_desc', 'col_desc'])


def _row_desc(rows, maxid, flavor):
    ids = np.random.randint(1, maxid, rows)

    timestamps = int(
        datetime.datetime(2012, 1, 1).strftime('%s')) + np.random.randint(1,
                                                                          2 * 365 * 24 * 3600,
                                                                          rows)

    spec = AxisDescription(columns=[timestamps, ids],
                           column_names=["timestamp", "id"], indices=["id"])

    if flavor == MetadataFlavor.PY_TABLES:
        return spec

    return pd.DataFrame({n: c for c, n in zip(spec.columns,
                                                  spec.column_names)})  # .set_index("id")


def _col_desc(cols, flavor):
    names = ['mycol{}'.format(i) for i in range(cols)]
    names_encoded = np.array([n.encode() for n in names])

    if flavor == MetadataFlavor.PY_TABLES:
        return AxisDescription(columns=[names_encoded],
                               column_names=["name"], indices=[])
    return pd.DataFrame({"name": names})


def _data(rows, cols):
    mat_shape = (rows, cols)

    mat = np.random.uniform(size=mat_shape)
    mat[0, 0] = np.nan
    mat[1, 0] = np.inf
    mat[0, 1] = np.infty

    return mat


def _create_bigmatrix_elements(rows, cols, maxid=10,
                               row_desc_flavor=MetadataFlavor.PY_TABLES,
                               col_desc_flavor=MetadataFlavor.PY_TABLES):
    assert rows >= 2 and cols >= 2, 'test matrix needs to be large enough'

    mat = _data(rows, cols)
    rd = _row_desc(rows, maxid, row_desc_flavor)
    cd = _col_desc(cols, col_desc_flavor)

    return BigMatrixElement(mat, rd, cd)


def _assert_bigmatrix_equal(elems, hf):
    mat_pt = hf.get_node('/{}'.format(bigmatrix.DATA_KEY))
    np.testing.assert_array_almost_equal(mat_pt, elems.matrix)


def _assert_axis_desc(orig, path, key):
    if isinstance(orig, pd.DataFrame):
        df_file = pd.read_hdf(path, key)
        pd.testing.assert_frame_equal(orig, df_file)
    elif isinstance(orig, AxisDescription):
        with tables.open_file(str(path), 'r') as hf:
            tbl = hf.get_node("/{}".format(key))

            for c, cn in zip(orig.columns, orig.column_names):
                np.testing.assert_array_equal(c, tbl.col(cn))

            assert set(tbl.indexedcolpathnames) == set(orig.indices)
    else:
        raise ValueError("type {} not supported".format(type(orig)))


def test_create_bigmatrix(tmpdir):
    elems = _create_bigmatrix_elements(10, 15)
    chunkshape = (5, 3)

    path = Path('/tmp', 'matrix_test.h5')
    if path.exists():
        path.unlink()
    writer = TablesBigMatrixWriter()
    writer.write(path, elems.matrix, elems.row_desc, elems.col_desc,
                 chunkshape=chunkshape)

    with tables.open_file(str(path), 'r') as hf:
        assert chunkshape == hf.get_node(
            '/{}'.format(bigmatrix.DATA_KEY)).chunkshape
        _assert_bigmatrix_equal(elems, hf)

    _assert_axis_desc(elems.row_desc, path, bigmatrix.ROW_DESC_KEY)
    _assert_axis_desc(elems.col_desc, path, bigmatrix.COL_DESC_KEY)


@pytest.mark.parametrize("row_desc_flavor",
                         [MetadataFlavor.PY_TABLES, MetadataFlavor.PANDAS_DF])
def test_create_bigmatrix_appending(tmpdir, row_desc_flavor):
    elems = _create_bigmatrix_elements(150, 15, row_desc_flavor=row_desc_flavor)
    chunkshape = (5, 5)
    increment_size = 10

    path = Path(tmpdir, 'matrix_test_append.h5')
    if path.exists():
        path.unlink()

    writer = TablesBigMatrixWriter()

    boundaries = np.arange(0, elems.matrix.shape[0] + 1, increment_size)

    for start, end in zip(boundaries[0:], boundaries[1:]):
        if row_desc_flavor == MetadataFlavor.PY_TABLES:
            row_desc = AxisDescription(
                columns=[e[start:end] for e in elems.row_desc.columns],
                column_names=elems.row_desc.column_names,
                indices=elems.row_desc.indices)
        else:
            row_desc = elems.row_desc.iloc[start:end]

        writer.write_or_append(path,
                               elems.matrix[start:end, ],
                               row_desc,
                               elems.col_desc, chunkshape=chunkshape)

    with tables.open_file(str(path), 'r') as hf:
        mat_pt = hf.get_node("/{}".format(bigmatrix.DATA_KEY))
        assert chunkshape == mat_pt.chunkshape

        _assert_bigmatrix_equal(elems, hf)

    _assert_axis_desc(elems.row_desc, path, bigmatrix.ROW_DESC_KEY)
    _assert_axis_desc(elems.col_desc, path, bigmatrix.COL_DESC_KEY)


@pytest.mark.parametrize("row_desc_flavor",
                         [MetadataFlavor.PY_TABLES, MetadataFlavor.PANDAS_DF])
@pytest.mark.parametrize("col_desc_flavor",
                         [MetadataFlavor.PY_TABLES, MetadataFlavor.PANDAS_DF])
def test_reading_bigmatrix(tmpdir, row_desc_flavor, col_desc_flavor):
    elems = _create_bigmatrix_elements(100, 5, maxid=5,
                                       row_desc_flavor=row_desc_flavor,
                                       col_desc_flavor=col_desc_flavor)

    if row_desc_flavor == MetadataFlavor.PY_TABLES:
        ids = np.array(elems.row_desc.columns[1])  # TODO: don't use magic
    else:
        ids = elems.row_desc["id"]

    elems.matrix[:, 0] = ids

    path = Path(tmpdir, 'bigmatrix_test_{}{}.h5'.format(row_desc_flavor,
                                                        col_desc_flavor))

    if path.exists():
        path.unlink()
    writer = TablesBigMatrixWriter()
    writer.write(path, elems.matrix, elems.row_desc, elems.col_desc)

    with TablesBigMatrixReader(path) as reader:
        np.testing.assert_array_almost_equal(elems.matrix, reader.data)

        mat = elems.matrix

        np.testing.assert_array_almost_equal(mat[0:3],
                                             reader[0:3])

        np.testing.assert_array_almost_equal(mat[5:7, 1:3], reader[5:7, 1:3])

        np.testing.assert_array_almost_equal(mat[np.ix_((5, 7), (1, 2, 3))],
                                             reader[(5, 7), (1, 2, 3)])

        np.testing.assert_array_almost_equal(mat[(5, 7), 1:3],
                                             reader[(5, 7), 1:3])

        np.testing.assert_array_almost_equal(mat[(5, 6, 7), 1:3],
                                             reader[(5, 6, 7), 1:3])

        np.testing.assert_array_almost_equal(mat[5:7, (1, 3)],
                                             reader[5:7, (1, 3)])

        np.testing.assert_array_almost_equal(mat[:, (1, 3)],
                                             reader[:, (1, 3)])

        row_idx = reader.get_row_indices('id == 4')

        assert (reader[row_idx, 0] == 4).all()

        assert (reader[row_idx, 0:1][0] == 4).all()

        cols0 = reader.get_col_indices_by_name(['mycol0', 'mycol3', 'mycol1'])
        assert (reader[row_idx, cols0][:, 0] == 4).all()

        cols1 = reader.get_col_indices_by_name(['mycol1', 'mycol0'])
        assert (reader[row_idx, cols1][:, 1] == 4).all()

        assert len(reader[(3,4,5), (0,2,4)]) > 0


@pytest.mark.parametrize("filter_name,expected",
                         [('default', 'blosc:lz4'),  # special marker
                          (None, None),
                          ('zlib', 'zlib')
                          ])
def test_create_bigmatrix_compression_filters(tmpdir, filter_name, expected):
    elems = _create_bigmatrix_elements(150, 15)

    path = Path(tmpdir, 'matrix.h5')

    if filter_name != 'default':
        if filter_name:
            filter = tables.Filters(complevel=1, complib=filter_name)
        else:
            filter = None

        TablesBigMatrixWriter().write(path, elems.matrix, elems.row_desc,
                                      elems.col_desc, compression_filter=filter)
    else:
        TablesBigMatrixWriter().write(path, elems.matrix, elems.row_desc,
                                      elems.col_desc)

    with tables.open_file(str(path), 'r') as hf:
        mat_pt = hf.get_node("/{}".format(bigmatrix.DATA_KEY))
        assert mat_pt.filters.complib == expected

@pytest.mark.parametrize("l,expected", [
    ([], []),
    ([1], slice(1,2)),
    ([1,2], slice(1,3)),
    ([1,2,5], [1,2,5]),
    ([1,5], [1,5]),
    ([1,2,3,4], slice(1,5))
])
def test_convert_slice(l, expected):
    assert TablesBigMatrixReader._convert_to_slice(l) == expected
