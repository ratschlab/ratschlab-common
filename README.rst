================
ratschlab common
================

..
   
   .. image:: https://img.shields.io/pypi/v/ratschlab_common.svg
           :target: https://pypi.python.org/pypi/ratschlab_common

   .. image:: https://img.shields.io/travis/ratschlab/ratschlab_common.svg
           :target: https://travis-ci.org/ratschlab/ratschlab_common

   .. image:: https://readthedocs.org/projects/ratschlab-common/badge/?version=latest
           :target: https://ratschlab-common.readthedocs.io/en/latest/?badge=latest
           :alt: Documentation Status


Small library of common code used in various projects in the `ratschlab
<http://www.ratschlab.org>`_.  

* Free software: MIT license


Features
--------

* Writing parquet and HDF5 files with sensible defaults.
* Support for working with 'chunkfiles', i.e. splitting up a large dataset in smaller chunks which can be processed independently:

  * Repartition records (i.e. increase or decrease number of chunkfiles) while keeping data belonging together in the same file (e.g. data with the same patient id associated)
  * simple indexing for looking up in which chunk to find data belonging e.g. to a patient

* bigmatrix: support for creating and reading large matrices stored in HDF5 having additional metadata on the axes in form of data frames.
* small wrappers for spark and dask (still under construction)
* Tool to dump database tables into parquet files (`export_db_to_files`). Large tables can be partitioned on a key and dumped into separate file chunks. This allows for further processing to be easily done in parallel.

