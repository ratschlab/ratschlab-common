"""
Utility functions to repartition chunkfiles. If a key is specified,
the records are repartitioned, s.t. the records having the same key are put
to the same partition, i.e. the same file.

The repartitioning is done using dask. Note, the functions return only a
computation graph, i.e. don't actually perform the repartition yet. The caller
has then to call the compute() function with an appropriate scheduler.

The repartition functions do some computations however, in case a partition
key is given, to determine the partition boundaries, i.e. determine which key
goes into which file
"""

import math
from pathlib import Path

import numpy as np


def repartition_directory(dir_path, output_path, n_partitions, key, df_format):
    """
    Takes a directory of chunkfiles and rearranges the records in a new
    directory into a specified number of chunkfiles. If a key is given,
    records having the same value at this key are put into the same partition.

    :param dir_path: str or pathlib object for input chunkfile directory
    :param output_path: str or pathlib object for output chunkfile directory
    :param n_partitions: positive int, number of final partitions
    :param key: str or None, partition key
    :param df_format: AbstractDataFrameFormat instance used for writing the
    resulting data frame
    :return: dask computation graph
    """
    ddf = df_format.read_dask_df(dir_path)

    ddf_rep = repartition_dask_dataframe(ddf, n_partitions, key)

    dest = Path(output_path)
    dest.mkdir(exist_ok=True)
    return df_format.write_dask_df(ddf_rep, dest)


def repartition_dask_dataframe(ddf, n_partitions, key):
    """
    Repartitions a dask dataframe into a given number of partitions. If a key
    is given, records with the having the same key end up in the same partition.

    :param ddf: dask data frame
    :param n_partitions: positive int, number of final partitions
    :param key: str or None, partition key
    :return: dask computation graph
    """
    if n_partitions == 1:
        return ddf

    if key:
        divs = _get_divisions(ddf, key, n_partitions)
        ret = (ddf.set_index(key).
               repartition(list(divs), force=True).
               reset_index())
    else:
        ret = ddf.repartition(npartitions=n_partitions)

    return ret


def _get_ids(ddf, key):
    return ddf[key].unique().compute()


def _compute_divisions(ids, parts):
    sorted_ids = np.sort(ids)
    step = math.ceil(len(sorted_ids) / parts)
    divs = sorted_ids[::step]
    max_elem = sorted_ids[-1]
    return np.unique(np.append(divs, max_elem))


def _get_divisions(ddf, key, n_divisions):
    ids = _get_ids(ddf, key)
    divs = _compute_divisions(ids, n_divisions)
    return divs
