import math
import logging

from pyspark.sql import SparkSession

from ratschlab_common.db.utils import PostgresDBConnectionWrapper, \
    PostgresDBParams


class PostgresTableDumper:
    def __init__(self, params: PostgresDBParams, spark_session: SparkSession,
                 partition_size: int = 512):
        """

        :param params:
        :param spark_session:
        :param partition_size: that much worth of data in MB on postgres
        would get stored into one partition
        """

        self.spark = spark_session
        self.params = params
        self.partition_size = partition_size

    def dump_table(self, table_name, path, partition_column=None,
                   nr_partitions=None):
        """

        :param table_name:
        :param path:
        :param partition_column: integer column!
        :param nr_partitions:
        :return:
        """
        # TODO: warnings if no partitions for large tables

        # TODO: warnings if partition not indexed

        if partition_column and not nr_partitions:
            # figure out number of partitions
            with PostgresDBConnectionWrapper(self.params) as db:
                tbl_size = db.table_size(table_name)

            nr_partitions = int(math.ceil(tbl_size /
                                          (self.partition_size * 1024 ** 2.0)))

        if partition_column:
            min_val, max_val = self._compute_min_max_values(table_name,
                                                            partition_column)

            df = self.spark.read.jdbc(self.params.jdbc_database_url(),
                                      table_name,
                                      column=partition_column,
                                      lowerBound=int(min_val),
                                      upperBound=int(max_val),
                                      numPartitions=nr_partitions,
                                      properties=self.params.to_jdbc_dict())
        else:
            df = self.spark.read.jdbc(self.params.jdbc_database_url(),
                                      table_name,
                                      properties=self.params.to_jdbc_dict())

        logging.info("Dumping to %s using %s partitions", str(path),
                     nr_partitions if nr_partitions else 1)

        if partition_column:
            df = df.sortWithinPartitions(partition_column)

        df.write.parquet(str(path))

    def _compute_min_max_values(self, table_name, col_name):
        with PostgresDBConnectionWrapper(self.params) as db:
            r = db.raw_query("SELECT MIN({}) AS min, MAX({}) AS max FROM {}".
                             format(col_name, col_name, table_name)).next()

            if not r['min'] or not r['max']:
                return 0, 1

            return int(r['min']), int(r['max'])
