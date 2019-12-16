from pathlib import Path

import pandas as pd
import pandas.testing
import pytest
import testing.postgresql
from tests.db import data_fixtures

from ratschlab_common import spark_wrapper
from ratschlab_common.db.dump_tables import PostgresTableDumper

@pytest.fixture()
def spark_session(scope="module"):
    return spark_wrapper.create_spark_session(1, 500)


@pytest.mark.parametrize("nr_partitions", [None, 1, 3])
@pytest.mark.slow
def test_dump_tables(simple_db, a_df: pd.DataFrame, tmpdir, spark_session,
                     nr_partitions):
    db_cfg, db_instance = simple_db

    out = Path(tmpdir, data_fixtures.simple_db_table_name)

    PostgresTableDumper(db_cfg, spark_session).dump_table(data_fixtures.simple_db_table_name, out,
                                                          nr_partitions=nr_partitions,
                                                          partition_column=data_fixtures.simple_db_partition_col)

    # read back
    df = pd.read_parquet(out)

    a_df_sorted = a_df.sort_values([data_fixtures.simple_db_partition_col, 'col0']).reset_index(
        drop=True)

    df_sorted = df.sort_values([data_fixtures.simple_db_partition_col, 'col0']).reset_index(
        drop=True)

    pandas.testing.assert_frame_equal(a_df_sorted, df_sorted)

    expected_partitions = nr_partitions if nr_partitions is not None else 1

    assert expected_partitions == len(list(out.glob('*.parquet')))
