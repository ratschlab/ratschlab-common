from pathlib import Path

import pandas as pd
import pandas.testing
import pytest
import testing.postgresql

from ratschlab_common import utils
from ratschlab_common.db.dump_tables import PostgresTableDumper
from ratschlab_common.db.utils import PostgresDBParams

_table_name = 'mytable'
_partition_col = 'col4'  # integer column in 'a_df' fixture


@pytest.fixture(scope="module")
def simple_db(a_df):
    db = testing.postgresql.Postgresql()

    db_params = PostgresDBParams(port=db.settings['port'], db='test',
                                 password='test')

    conn = db_params.connection()
    a_df.to_sql(_table_name, conn, index=False)

    return db_params, db


@pytest.fixture()
def spark_session(scope="module"):
    return utils.create_spark_session(1, 500)


@pytest.mark.parametrize("nr_partitions", [None, 1, 3])
@pytest.mark.slow
def test_dump_tables(simple_db, a_df: pd.DataFrame, tmpdir, spark_session,
                     nr_partitions):
    db_cfg, db_instance = simple_db

    out = Path(tmpdir, _table_name)

    PostgresTableDumper(db_cfg, spark_session).dump_table(_table_name, out,
                                                          nr_partitions=nr_partitions,
                                                          partition_column=_partition_col)

    # read back
    df = pd.read_parquet(out)

    a_df_sorted = a_df.sort_values([_partition_col, 'col0']).reset_index(
        drop=True)

    df_sorted = df.sort_values([_partition_col, 'col0']).reset_index(
        drop=True)

    pandas.testing.assert_frame_equal(a_df_sorted, df_sorted)

    expected_partitions = nr_partitions
    if not nr_partitions:
        expected_partitions = 1

    assert expected_partitions == len(list(out.glob('*.parquet')))
