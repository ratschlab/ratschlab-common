import math
from pathlib import Path

import numpy as np


def repartition_directory(dff, dir_path, output_path, n_partitions, key):
    ddf = dff.read_dask_df(dir_path)

    ddf_rep = repartition_dask_dataframe(ddf, n_partitions, key)

    dest = Path(output_path)
    dest.mkdir(exist_ok=True)
    return dff.write_dask_df(ddf_rep, dest)


def repartition_dask_dataframe(ddf, n_partitions, key):
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
