import logging
import sys
from pathlib import Path

import click
from pyspark.sql import SparkSession

import ratschlab_common.utils
from ratschlab_common.db.dump_tables import PostgresTableDumper
from ratschlab_common.db.utils import PostgresDBConnectionWrapper, \
    PostgresDBParams

logging.basicConfig(level=logging.INFO)


def _row_counts_parquet(path: Path, spark: SparkSession) -> int:
    return spark.read.parquet(str(path)).count()


def row_counts_match(path: Path, table_name: str, db_params:
PostgresDBParams, spark: SparkSession):
    row_counts = _row_counts_parquet(path, spark)

    with PostgresDBConnectionWrapper(db_params) as db_wrapper:
        approx_cnt = db_wrapper.count_rows(table_name, approx=True)

        if row_counts != approx_cnt:
            logging.warning("Approximate counts didn't match, trying exact "
                            "counts. May take a while....")
            exact_cnt = db_wrapper.count_rows(table_name, approx=False)

            if row_counts != exact_cnt:
                logging.error("Counts don't match: got %s in DB but %s in "
                              "file", row_counts, exact_cnt)

            return row_counts == exact_cnt
        return True


@click.command()
@click.argument("dest-dir", type=click.Path(writable=True))
@click.option("--db-host", type=str,
              help='Database host', default='localhost')
@click.option("--db-port", type=int,
              help='Database port', default=5432)
@click.option('--db-name', type=str)
@click.option('--db-schema', type=str, default='public')
@click.option("--db-username", type=str,
              help='Database username (password should be managed via .pgpass')
@click.option("--ssl-mode", type=str,
              help='SSL Mode', default='disable')
@click.option("--cores", type=int, default=1)
@click.option("--memory-per-core", type=int, default=5000)
@click.option("--force", is_flag=True, default=False,
              help='If set, table files will be overwritten')
@click.option('--default-partition-col', type=str, default=None)
@click.option('--partition-col', type=(str, str), multiple=True)
@click.option('--nr-partitions', type=(str, int), multiple=True)
def main(dest_dir, db_host, db_port, db_name, db_schema, db_username, ssl_mode, force,
         cores,
         memory_per_core, default_partition_col, partition_col, nr_partitions):
    partition_col_dict = {k: v for k, v in partition_col}
    nr_partitions_dict = {k: v for k, v in nr_partitions}

    dest_dir_path = Path(dest_dir)
    dest_dir_path.mkdir(exist_ok=True, parents=True)

    db_params = PostgresDBParams(user=db_username, host=db_host,
                                 port=db_port, db=db_name, schema=db_schema, ssl_mode=ssl_mode)

    db_wrapper = PostgresDBConnectionWrapper(db_params)
    tables = db_wrapper.list_tables()
    
    with ratschlab_common.utils.create_spark_session(cores, memory_per_core) \
        as spark:

        dumper = PostgresTableDumper(db_params, spark)
        for t in tables:
            logging.info('Dumping table %s', t)

            tbl_path = Path(dest_dir_path, t)

            if not tbl_path.exists() and not force:
                default_col = None

                if default_partition_col and default_partition_col in \
                    db_wrapper.list_columns(t):
                    default_col = default_partition_col

                p_col = partition_col_dict.get(t, default_col)
                nr_part = nr_partitions_dict.get(t, None)

                dumper.dump_table(t, tbl_path, p_col, nr_part)
            else:
                logging.info('Path %s already exists, not dumping table %s',
                             tbl_path, t)

            counts_match = row_counts_match(tbl_path, t, db_params, spark)

            if counts_match:
                logging.info("Counts for %s match", t)
            else:
                logging.error("Counts for %s don't match", t)

    db_wrapper.close()


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    sys.exit(main())
