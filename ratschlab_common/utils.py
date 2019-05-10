from typing import Any, Sequence
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from distributed import LocalCluster
from pathlib import Path
import tempfile


def default_spark_config(cores: int, memory_per_executor: int, driver_overhead: int = 2000, tmp_dir: str = '', extra_java_options: str = '', enable_arrow: bool = True) -> SparkConf:
    driver_mem = cores * memory_per_executor + driver_overhead

    jars = [str(p) for p in Path(Path(__file__).parent.parent, "jars").glob(
        '*.jar')]
    jar_paths = ':'.join(jars)

    cfg = SparkConf()

    if tmp_dir:
        cfg.set("spark.local.dir", tmp_dir)

    # avoiding trouble with JDBC and timestamps
    java_options = "-Duser.timezone=UTC " + str(extra_java_options)

    return (cfg.set("spark.driver.memory", "{}m".format(driver_mem)).
            set("spark.executor.memory", "{}m".format(memory_per_executor)).
            set("spark.driver.extraJavaOptions", java_options).
            set("spark.master", "local[{}]".format(cores)).
            set("spark.jars", jar_paths).
            set("spark.sql.execution.arrow.enabled", str(enable_arrow))
            )


def create_spark_session(cores: int, memory_per_executor: int) -> SparkSession:
    return create_spark_session_from_config(default_spark_config(cores, memory_per_executor))


def create_spark_session_from_config(cfg: SparkConf) -> SparkSession:
    return (SparkSession.
             builder.
             config(conf=cfg).
             getOrCreate())


def create_dask_cluster(cores: int, memory_per_worker: int):
    """

    :param cores:
    :param memory_per_worker: not a hard limit.
    :return:
    """

    worker_args = {'local_dir': str(Path(tempfile.gettempdir(), 'dask_dir')),
                   'memory_limit': str(memory_per_worker * 1024 ** 2)}

    return LocalCluster(cores, threads_per_worker=1, **worker_args)


def chunker(seq: Sequence[Any], size: int):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
