import os
from abc import ABCMeta, abstractmethod
from types import MappingProxyType

import dask.dataframe
import pandas as pd


class AbstractDataFrameFormat:
    """
    Provides helper functions to serialize and deserialize pandas and
    dask dataframes.

    Instances of such classes can define their own default options via the
    constructor. If none are specified, the library default are taken as
    default options. Default options can be overwritten in the calling
    functions.
    """
    __metaclass__ = ABCMeta

    def __init__(self, default_reading_options=None,
                 default_writing_options=None):
        self.default_reading_options = default_reading_options
        self.default_writing_options = default_writing_options

    @abstractmethod
    def read_df(self, file_path, options=None):
        raise NotImplementedError

    @abstractmethod
    def write_df(self, df, file_path, options=None):
        raise NotImplementedError

    @abstractmethod
    def read_dask_df(self, directory, options=None):
        raise NotImplementedError

    @abstractmethod
    def write_dask_df(self, ddf, directory, options=None,
                      compute=False):
        raise NotImplementedError

    @abstractmethod
    def format_extension(self):
        raise NotImplementedError

    @abstractmethod
    def _library_default_reading_options(self):
        raise NotImplementedError

    @abstractmethod
    def _library_default_writing_options(self):
        raise NotImplementedError

    def get_default_reading_options(self):
        """
        Default options used, if no options are passed in the read functions

        :return: dictionary with options
        """
        if self.default_reading_options:
            return self.default_reading_options
        return self._library_default_reading_options()

    def get_default_writing_options(self):
        """
        Default options used, if no options are passed in the write functions

        :return: dictionary with options
        """
        if self.default_writing_options:
            return self.default_writing_options
        return self._library_default_writing_options()

    @staticmethod
    def _get_effective_options(params, defaults):
        if not params:
            effective_params = defaults
        else:
            effective_params = dict(defaults, **params)

        return effective_params


class ParquetDataFrameFormat(AbstractDataFrameFormat):
    DEFAULT_ENGINE = 'pyarrow'

    def read_df(self, file_path, options=None):
        e = ParquetDataFrameFormat._get_effective_options(options,
                                                          self.get_default_reading_options())
        return pd.read_parquet(file_path, **e)

    def write_df(self, df, file_path, options=None):
        e = self._get_effective_options(options,
                                        self.get_default_writing_options())
        df.to_parquet(file_path, **e)

    def read_dask_df(self, directory, options=None):
        in_pattern = os.path.join(directory,
                                  '*.{}'.format(self.format_extension()))
        e = ParquetDataFrameFormat._get_effective_options(options,
                                                          self.get_default_reading_options())
        return dask.dataframe.read_parquet(str(in_pattern), **e)

    def write_dask_df(self, ddf, directory, options=None,
                      compute=False):
        e = ParquetDataFrameFormat._get_effective_options(options,
                                                          self.get_default_writing_options())
        return ddf.to_parquet(str(directory), compute=compute, **e)

    def format_extension(self):
        return "parquet"

    def _library_default_reading_options(self):
        return MappingProxyType({'engine': self.DEFAULT_ENGINE})

    def _library_default_writing_options(self):
        return MappingProxyType({'engine': self.DEFAULT_ENGINE,
                                 'compression': 'snappy'})


class HdfDataFrameFormat(AbstractDataFrameFormat):
    DEFAULT_KEY = 'data'

    def read_df(self, file_path, options=None):
        eff = HdfDataFrameFormat._get_effective_options(options,
                                                        self.get_default_reading_options())
        return pd.read_hdf(file_path, **eff)

    def write_df(self, df, file_path, options=None):
        eff = HdfDataFrameFormat._get_effective_options(options,
                                                        self.get_default_writing_options())
        df.to_hdf(file_path, **eff)

    def read_dask_df(self, directory, options=None):
        in_pattern = os.path.join(directory,
                                  '*.{}'.format(self.format_extension()))
        e = HdfDataFrameFormat._get_effective_options(options,
                                                      self.get_default_reading_options())
        return dask.dataframe.read_hdf(str(in_pattern), **e)

    def write_dask_df(self, ddf, directory, options=None,
                      compute=False):
        e = HdfDataFrameFormat._get_effective_options(options,
                                                      self.get_default_writing_options())
        out_pattern = os.path.join(directory,
                                   'part-*.{}'.format(self.format_extension()))
        return ddf.to_hdf(out_pattern, compute=compute, **e)

    def format_extension(self):
        return 'h5'

    def _library_default_reading_options(self):
        return MappingProxyType({'mode' : 'r', 'key': self.DEFAULT_KEY})

    def _library_default_writing_options(self):
        return MappingProxyType({'key': self.DEFAULT_KEY, 'format': 'table',
                                 'complib': 'blosc:lz4', 'complevel': 5,
                                 'fletcher32': True})
