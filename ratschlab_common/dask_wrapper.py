from pathlib import Path
import tempfile
from distributed import LocalCluster


def create_dask_cluster(cores: int, memory_per_worker: int):
    """

    :param cores:
    :param memory_per_worker: not a hard limit.
    :return:
    """

    worker_args = {'local_dir': str(Path(tempfile.gettempdir(), 'dask_dir')),
                   'memory_limit': str(memory_per_worker * 1024 ** 2)}

    return LocalCluster(cores, threads_per_worker=1, **worker_args)
