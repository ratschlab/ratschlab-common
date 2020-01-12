import pandas as pd
import numpy as np
from pandas.api.types import is_sparse
from scipy import sparse
import tables

# need to check which sparse matrix to use (for now supports only csr_matrix)


class SparseDFReader:

    def __init__(self, path):
        self.path = path
        self.f = tables.open_file(path, mode='r')

    def read_hdf(self):
        sparse_group = self.f.get_node("/data/sparse")
        sparse_m = self._read_sparse_m(sparse_group)
        s_col_names = self.f.get_node("/data/sparse/col_names")
        non_sparse = pd.read_hdf(self.path, key="/data/non_sparse")
        sparse = pd.DataFrame.sparse.from_spmatrix(sparse_m, columns=s_col_names)
        return pd.concat([sparse, non_sparse], axis=1)

    @staticmethod
    def _read_sparse_m(group):
        attributes = []
        for attribute in ('data', 'indices', 'indptr', 'shape'):
            attributes.append(getattr(group, attribute).read())
        sparse_m = sparse.csr_matrix(tuple(attributes[:3]), shape=attributes[3])
        return sparse_m

class SparseDFWriter:

    def __init__(self, df):

        # from nan to zeros (pandas sparse array wants nan by default, scipy sparse matrix zeros)
        # self.df = df.fillna(0.0)
        self.df = df

        # Getting sparse part and non sparse part
        self.sparse, self.non_sparse = self._split_sparse()

        # Create sparse matrix
        self.sparse_m = sparse.csr_matrix(self.sparse.values)

    @staticmethod
    def _store_sparse_m(m, hdf5, group):
        assert (m.__class__ == sparse.csr.csr_matrix), 'M must be a csr matrix'
        for attribute in ('data', 'indices', 'indptr', 'shape'):
            arr = np.array(getattr(m, attribute))
            SparseDFWriter._store_array_hdf(arr, hdf5, group, attribute)
        return hdf5

    @staticmethod
    def _store_array_hdf(arr, hdf5, group, name):
        atom = tables.Atom.from_dtype(arr.dtype)
        ds = hdf5.create_carray(group, name, atom, arr.shape)
        ds[:] = arr


    def to_hdf(self, path):
        f = tables.open_file(path, mode='w')
        data_group = f.create_group("/", "data")
        data_sparse_group = f.create_group(data_group, "sparse")
        self._store_sparse_m(self.sparse_m, f, data_sparse_group)
        self._store_array_hdf(self.sparse.columns.values, f, data_sparse_group, "col_names")
        self.non_sparse.to_hdf(path, key="data/non_sparse")
        f.close()


    def _split_sparse(self):
        sparse_col = []
        non_sparse_col = []
        for col in self.df.columns:
            if is_sparse(self.df[col]):
                sparse_col.append(col)
            else:
                non_sparse_col.append(col)
        return self.df[sparse_col], self.df[non_sparse_col]




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
    writer = SparseDFWriter(df)
    writer.to_hdf(path)

    # create reader and read df
    reader = SparseDFReader(path)
    df = reader.read_hdf()
    print(df.head())
    print(df.dtypes)

    '''
    print(writer.sparse_m.data, writer.sparse_m.indices, writer.sparse_m.indptr, writer.sparse_m.shape)
    sdf = df.astype(pd.SparseDtype("float", np.nan))
    print(sdf.sparse.density)
    '''
