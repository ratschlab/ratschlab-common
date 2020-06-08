# ratschlab-common

Small library of common code used in various projects in the [ratschlab](http://www.ratschlab.org).

## Features

-   Writing parquet and HDF5 files with sensible defaults
    `ratschlab_common.io.dataframe_formats`.
-   Support for working with \'chunkfiles\', i.e. splitting up a large
    dataset in smaller chunks which can be processed independently (see
    [example notebook](https://github.com/ratschlab/ratschlab-common/blob/master/notebooks/chunkfiles_example.ipynb)):
    -   Repartition records (i.e. increase or decrease number of
        chunkfiles) while keeping data belonging together in the same
        file (e.g. data with the same patient id associated)
    -   simple indexing for looking up in which chunk to find data
        belonging e.g. to a patient
-   bigmatrix: support for creating and reading large matrices stored in
    HDF5 having additional metadata on the axes in form of data frames
    (see [example notebook](https://github.com/ratschlab/ratschlab-common/blob/master/notebooks/bigmatrix_example.ipynb).)
-   small wrappers for spark and dask ([spark
    example](https://github.com/ratschlab/ratschlab-common/blob/master/notebooks/spark_example.ipynb).)
-   saving sparse `pandas` dataframes to hdf5, see [example notebook](https://github.com/ratschlab/ratschlab-common/blob/master/notebooks/sparse_dataframe_io_example.ipynb)
    
### Tools

`ratschlab-common` also comes with some command line tools:

-   `pq-tool`: inspect parquet files on the command line
    -  `pq-tool head`: first records
    -  `pq-tool tail`: last records
    -  `pq-tool cat`: all records
    -  `pq-tool schema`: schema of a parquet file
-   `export-db-to-files`: Tool to dump (postgres) database tables into parquet files. Large tables can be
    partitioned on a key and dumped into separate file chunks. This
    allows for further processing to be easily done in parallel.
-   `bigmatrix-repack`: rechunking/packing bigmatrix hdf5 files

## Installation and Requirements

The library along with all the required dependencies can be installed
with:
```
pip install ratschlab-common[complete]
```

Depending on whether you plan to use `spark` or `dask` or none of them you could install
`ratschlab-common` through either of the commands
```
pip install ratschlab-common
pip install ratschlab-common[spark]
pip install ratschlab-common[dask]
```

Note, that if you plan on using `spark` make sure, you have
Java 8 and either python 3.6 or 3.7 installed (python 3.8 is currently not supported by `pyspark`).
