from typing import Any, Sequence
from pyspark.sql import SparkSession
from distributed import LocalCluster
from pathlib import Path
import tempfile

def create_spark_session(cores: int, memory_per_core: int):
    driver_mem = cores * memory_per_core + 2000 # ok? parametrize?

    jars = [str(p) for p in Path(Path(__file__).parent.parent, "jars").glob(
        '*.jar')]
    jar_paths = ':'.join(jars)

    return(SparkSession.
             builder.
             config("spark.driver.memory", "{}m".format(driver_mem)).
             config("spark.jars", jar_paths).
             config("spark.executor.memory", "{}m".format(memory_per_core)).
             config("spark.master", "local[{}]".format(cores)).
             getOrCreate())


def create_dask_cluster(cores: int, memory_per_core: int):
    """

    :param cores:
    :param memory_per_core: not a hard limit.
    :return:
    """

    worker_args = {'local_dir': str(Path(tempfile.gettempdir(), 'dask_dir')),
                   'memory_limit': str(memory_per_core*1024**2)}

    return LocalCluster(cores, threads_per_worker=1, **worker_args)


def chunker(seq: Sequence[Any], size: int):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
