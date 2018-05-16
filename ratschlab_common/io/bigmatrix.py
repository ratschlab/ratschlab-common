"""
'Bigmatrix' represents an abstraction for out-of-core computation with large
matrices having some 'metadata' associated with the rows and columns. This
metadata can be for example some patient id and a timestamp for the rows
and a column name for each column, e.g. the name of features. The matrix itself
could represent the values of the features for patients at different times.

The motivation behind this abstraction is, that a dataset can almost be
represented as a matrix, i.e. has the same datatype across all columns, typically
some float or int. But a few columns represent something else, e.g. time information
or ids. The idea is to handle these metadata separately from the rest. This way
the entire dataset can be handled more efficiently.
"""

import os
from abc import ABCMeta, abstractmethod
import collections
from collections import namedtuple
from enum import Enum

import numpy as np
import pandas as pd
import tables

DATA_KEY = 'data'
COL_DESC_KEY = 'col_descr'
ROW_DESC_KEY = 'row_descr'

NAME_FIELD = 'name'


class AbstractBigMatrixReader:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __getitem__(self, item):
        pass

    @abstractmethod
    def row_desc(self):
        """
        :return: pandas dataframe of row metadata
        """
        pass

    @abstractmethod
    def col_desc(self):
        """
        :return: pandas dataframe of column metadata
        """
        pass


class AbstractBigMatrixWriter:
    __metaclass__ = ABCMeta

    @abstractmethod
    def write(self, path, data, row_desc, col_desc):
        pass

    @abstractmethod
    def write_or_append(self, path, data, row_desc, col_desc):
        pass


class AxisDescription(
        namedtuple("AxisDescription", ['columns', 'column_names', 'indices'])):
    """
    Axis description, i.e. Metadata for an axis such as rows or columns.

    :param columns: list of 1-D numpy array, one element per column
    :param column_names: list of strings representing the column names of the
                         metadata table
    :param indices: list of column names which should be indexed
    """


class MetadataFlavor(Enum):
    """
    Whether the metadata on an axis is serialized as vanilla pytables or
    via pandas pytables.
    """
    PY_TABLES = 0
    PANDAS_DF = 1


class TablesBigMatrixWriter(AbstractBigMatrixWriter):
    """
    Writing a bigmatrix to HDF5 via pytables.

    Data is written, s.t. rows can be appended later, i.e. both rows in the actual
    matrix as well as rows in the row metadata table/dataframe.
    This allows also for writing a bigmatrix file incrementally.


    Important: When writing files
    incrementally, it is important to set a suitable chunkshape manually, as
    pytables will likely assume a too small chunksize based on the first write.

    Both metadata and matrix are stored using the BLOSC compressor by default.
    """

    BLOSC_FILTER = tables.Filters(complevel=5, complib='blosc:lz4',
                                  fletcher32=True)

    def write(self, path, data_matrix, row_desc, col_desc, chunkshape=None,
              compression_filter=BLOSC_FILTER):
        """
        Writes a matrix with row and column description into a hdf5 file

        :param path: path of the file
        :param data_matrix: numpy matrix
        :param row_desc: pandas dataframe or MetaDataSpec object describing the rows
        :param col_desc: pandas dataframe or MetaDataSpec object describing the columns
                         needs to contain some "name" field
        :param chunkshape: HDF chunkshape for storing the data matrix. If none is
        given it is determined by pytables.
        :param compression_filter: tables.Filters instance, by default doing blosc compression
        """
        with tables.open_file(str(path), mode='w') as hf:
            c = hf.create_earray(hf.root,
                                 DATA_KEY,
                                 atom=tables.Atom.from_dtype(
                                     data_matrix.dtype),
                                 shape=(0, data_matrix.shape[1]),
                                 filters=compression_filter,
                                 chunkshape=chunkshape)
            c.append(data_matrix)

        if isinstance(col_desc, AxisDescription):
            with tables.open_file(str(path), mode='a') as hf:
                self._write_pytables_metadata(col_desc, hf, COL_DESC_KEY,
                                              compression_filter)
        elif isinstance(col_desc, pd.DataFrame):
            self._write_pandas_metadata(col_desc, path, COL_DESC_KEY)
        else:
            raise ValueError("{} not supported".format(type(col_desc)))

        if isinstance(row_desc, AxisDescription):
            with tables.open_file(str(path), mode='a') as hf:
                self._write_pytables_metadata(row_desc, hf, ROW_DESC_KEY,
                                              compression_filter)
        elif isinstance(row_desc, pd.DataFrame):
            self._write_pandas_metadata(row_desc, path, ROW_DESC_KEY)
        else:
            raise ValueError("{} not supported".format(type(row_desc)))

    def write_or_append(self, path, data_matrix, row_desc, col_desc=None,
                        chunkshape=None, compression_filter=BLOSC_FILTER):
        """
        Writes a matrix with row and column description into a hdf5 file. If the
        file already exists, data is appended.

        :param path: path of the file
        :param data_matrix: numpy matrix
        :param row_desc: pandas dataframe or MetaDataSpec object describing the rows
        :param col_desc: pandas dataframe or MetaDataSpec object describing the columns
                         needs to contain some "name" field. If data gets appended
                         then it doesn't need to be provided.
        :param chunkshape: HDF chunkshape for storing the data matrix. If none is
        given it is determined by pytables.
        :param compression_filter: tables.Filters instance, by default doing blosc compression
        """
        if not os.path.exists(path):
            self.write(path, data_matrix, row_desc, col_desc,
                       chunkshape=chunkshape,
                       compression_filter=compression_filter)
            return

        with tables.open_file(str(path), mode='a') as hf:
            required_names = {DATA_KEY, COL_DESC_KEY, ROW_DESC_KEY}

            # first time it is called for a function.
            if all(('/{}'.format(n) in hf) for n in required_names):
                hf.get_node('/{}'.format(DATA_KEY)).append(data_matrix)

                if isinstance(row_desc, AxisDescription):
                    column_dict = {n: c for n, c in
                                   zip(row_desc.column_names, row_desc.columns)}

                    row_tbl = hf.get_node('/{}'.format(ROW_DESC_KEY))
                    lst = [column_dict[n]
                           for n in row_tbl.description._v_names]
                    row_tbl.append(lst)

                elif isinstance(row_desc, pd.DataFrame):
                    hf.close()
                    row_desc.to_hdf(path, ROW_DESC_KEY, mode='a', append=True,
                                    format='table')
                else:
                    raise ValueError("{} not supported".format(type(row_desc)))
            else:
                node_names = [x.name for x in hf.iter_nodes('/')]
                raise ValueError("expected nodes {} but seeing only {}".format(
                    required_names, node_names))

    def _write_pytables_metadata(self, desc, hf, key, compression_filter):
        py_desc = self._get_pytable_desc(desc.columns, desc.column_names)

        tbl = hf.create_table(hf.root, key,
                              py_desc,
                              filters=compression_filter)

        for ind_col in desc.indices:
            tbl.colinstances[ind_col].create_index()

        column_dict = {n: c for n, c in zip(desc.column_names, desc.columns)}
        lst = [column_dict[n] for n in py_desc._v_names]

        tbl.append(lst)

    def _write_pandas_metadata(self, df, path, key):
        df.to_hdf(path, key=key, mode='a', format='table')

    def _get_pytable_desc(self, col_list, col_names):
        d = {}
        for c, cn in zip(col_list, col_names):
            col_type = tables.Col.from_dtype(c.dtype)
            d[cn] = col_type

        return tables.Description(d)


class TablesBigMatrixReader(AbstractBigMatrixReader):
    """
    Reading bigmatrix entries and metadata from HDF5 via pytables
    """

    def __init__(self, file_path):
        """
        Opens a bigmatrix file
        :param file_path: path to the bigmatrix file (pathlib Path or string)
        """
        self.file_path = file_path
        self._f = tables.open_file(str(file_path), mode='r')

        self.row_desc_node = self._f.get_node("/{}".format(ROW_DESC_KEY))
        self.col_desc_node = self._f.get_node("/{}".format(COL_DESC_KEY))
        self.data = self._f.get_node("/{}".format(DATA_KEY))

        self._row_desc_df = None
        self._col_desc_df = None

        self._row_flavor = \
            MetadataFlavor.PANDAS_DF if 'table' in self.row_desc_node else MetadataFlavor.PY_TABLES
        self._col_flavor =\
            MetadataFlavor.PANDAS_DF if 'table' in self.col_desc_node else MetadataFlavor.PY_TABLES

        self.col_names = self._read_cols()

        self._col_name_to_index = {n: i for i, n in enumerate(self.col_names)}

    def __getitem__(self, item):
        if isinstance(item, slice) or len(item) == 1:
            return self.data[item]
        elif len(item) == 2:
            row_dims, col_dims = item

            # attempt to convert to slice (e.g. 1,2,3 -> 1:3)
            if isinstance(row_dims, collections.Iterable):
                row_dims = self._convert_to_slice(row_dims)

            if isinstance(col_dims, collections.Iterable):
                col_dims = self._convert_to_slice(col_dims)

            if isinstance(row_dims, slice) or isinstance(col_dims, slice):
                return self.data[row_dims, col_dims]

            # this can be fairly inefficient...
            # https://github.com/PyTables/PyTables/issues/401
            return self.data[row_dims, :][:, col_dims]
        else:
            raise IndexError('Invalid slices {}'.format(item))

    @staticmethod
    def _convert_to_slice(iterable):
        start = None
        last = None
        for elem in iterable:
            if not start:
                start = elem
            if last and elem - last != 1:
                return iterable
            last = elem

        if start is None:
            return iterable
        return slice(start, last+1)

    def get_row_indices(self, row_query_string):
        """
        Determines row indices where the row metadata satisfies certain constraints.

        If the metadata is stored in vanilla pytables, tables.get_where_list is
        isued. In case of a pandas dataframe, pandas.DataFrame.query is used.
        Please refer to the respective documentation for details.

        :param row_query_string: query string
        :return: np.ndarray containing integer indices
        """
        if self._row_flavor == MetadataFlavor.PY_TABLES:
            return self.row_desc_node.get_where_list(row_query_string)

        return self.row_desc().query(row_query_string).index.values

    def get_col_indices_by_name(self, col_list):
        """
        Determines column indices of a list of column names. Assumes that the
        col metadata table/dataframe has a column 'name'

        :param col_list: list of strings representing column names
        :return: np.ndarray containing integer indices
        """
        return np.array(list(self._col_name_to_index[cn] for cn in col_list))

    def row_desc(self):
        """
        Row metadata as pandas dataframe.

        The dataframe is kept in memory.
        :return: pandas dataframe
        """
        if not isinstance(self._row_desc_df, pd.DataFrame):
            self._row_desc_df = (pd.read_hdf(self.file_path, key=ROW_DESC_KEY).
                                 reset_index(drop=True))
        return self._row_desc_df

    def col_desc(self):
        """
        Column metadata as pandas dataframe.

        The dataframe is kept in memory.

        :return: pandas dataframe
        """
        if not isinstance(self._col_desc_df, pd.DataFrame):
            self._col_desc_df = pd.read_hdf(self.file_path, key=COL_DESC_KEY)
        return self._col_desc_df

    def row_desc_as_table(self):
        """
        Row metadata as pytable
        :return: table object
        """
        return self._f.get_node("/{}".format(ROW_DESC_KEY))

    def col_desc_as_table(self):
        """
        Column metadata as pytable
        :return: table object
        """
        return self._f.get_node("/{}".format(COL_DESC_KEY))

    def close(self):
        """
        Closes file
        """
        self._f.close()

    def _read_cols(self):
        if self._col_flavor == MetadataFlavor.PY_TABLES:
            tbl = self.col_desc_as_table()

            cols_bytes = tbl.read(field=NAME_FIELD)
            return np.array([b.decode('UTF-8') for b in cols_bytes])

        return self.col_desc()[NAME_FIELD].values

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
