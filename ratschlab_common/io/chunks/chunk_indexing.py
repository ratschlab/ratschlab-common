"""
Some initial code to create and load file indices for chunked datafiles.
The indices store in which file to find the data associated to a certain value
of some field, e.g. in which file the data of some patient with a specific
patientid.
"""

import glob
import json
import os

import dask
import dask.bag
import dask.dataframe


def index_chunkfiles(directory, key, df_format, index_file_path=None,
                     num_workers=4):
    """Indices a directory of chunk files.

    :param directory: path to directory
    :param key: field to index on (e.g. 'patientid')
    :param df_format: instance of AbstractDataFrameFormat
    :param index_file_path: path for the resulting index file. by default it is
    put in `directory`
    :param num_workers: how many subprocesses to use to index files in parallel
    """
    files = glob.glob(
        os.path.join(directory, '*.{}'.format(df_format.format_extension())))

    file_mappings_dsk = (
        dask.bag.from_sequence(files).
        map(lambda f: _get_map(f, key, df_format)).
        fold(_merge_maps)
    )

    file_mappings = file_mappings_dsk.compute(num_workers=num_workers)

    if not index_file_path:
        index_file_path = os.path.join(
            directory, "_{}_index.json".format(key))

    _write_index(index_file_path, file_mappings)

    return index_file_path


def load_chunkfile_index(index_file):
    """ Loads an index file as a dictionary from key to filename.
    Note, that the keys in the dictionary are string objects, ie. if you look
    for patientid 42, then the query is my_dict[str(42)]

    :param index_file: path to the index file
    """
    with open(index_file, 'r') as f:
        return json.load(f, encoding='utf-8')


def _get_map(file_path, key, df_format):
    df = df_format.read_df(file_path, {'columns': [key]})

    keys = df[key].unique()
    file_name = os.path.basename(file_path)
    return {str(k): file_name for k in keys}


def _merge_maps(m1, m2):
    if not m1.keys().isdisjoint(m2):
        msg = "Maps are supposed to have disjoint key sets!" \
              "got {} and {}".format(m1, m2)
        raise ValueError(msg)

    result = {}

    result.update(m1)
    result.update(m2)
    return result


def _write_index(file_name, mappings):
    with open(file_name, 'w') as f:
        json.dump(mappings, f)
