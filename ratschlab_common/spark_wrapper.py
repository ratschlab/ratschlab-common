from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pathlib import Path


def default_spark_config(cores: int, memory_per_executor: int, driver_overhead: int = 2000,
                         tmp_dir: str = '', extra_java_options: str = '', enable_arrow: bool = True, use_utc: bool= False) -> SparkConf:
    '''
    Constructs SparkConf object with sensible defaults for the ratschlab environment

    See also https://spark.apache.org/docs/latest/configuration.html for more information
    about the semantics of the configurations

    :param cores: number of executors (workers)
    :param memory_per_executor: memory per executor [MB]
    :param driver_overhead: Memory to allocate for the driver [MB], excluding exector memory
    :param tmp_dir: "scratch" space for spark, typically /tmp by default
    :param extra_java_options: extra parameters for the JVM
    :param enable_arrow: see https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#pyspark-usage-guide-for-pandas-with-apache-arrow , requires pyarrow to be installed
    :return: SparkConf instance
    '''
    driver_mem = cores * memory_per_executor + driver_overhead

    jars = [str(p) for p in Path(Path(__file__).parent.parent, "jars").glob(
        '*.jar')]
    jar_paths = ':'.join(jars)

    cfg = SparkConf()

    if tmp_dir:
        cfg.set("spark.local.dir", tmp_dir)

    java_options = str(extra_java_options)

    if use_utc:
        # avoiding trouble with JDBC and timestamps
        java_options = "-Duser.timezone=UTC " + java_options

    return (cfg.set("spark.driver.memory", "{}m".format(driver_mem)).
            set("spark.executor.memory", "{}m".format(memory_per_executor)).
            set("spark.driver.extraJavaOptions", java_options).
            set("spark.master", "local[{}]".format(cores)).
            set("spark.jars", jar_paths).
            set("spark.sql.execution.arrow.enabled", str(enable_arrow))
            )


def create_spark_session(cores: int, memory_per_executor: int) -> SparkSession:
    '''
    Creates a local spark session with a given number of executors and memory

    :param cores: number of executors (workers)
    :param memory_per_executor: memory per executor [MB]. A default overhead for the driver is added
    :return: SparkSession instance
    '''
    return create_spark_session_from_config(default_spark_config(cores, memory_per_executor))


def create_spark_session_from_config(cfg: SparkConf) -> SparkSession:
    return (SparkSession.
             builder.
             config(conf=cfg).
             getOrCreate())
