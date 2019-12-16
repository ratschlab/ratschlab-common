import pytest
import pandas as pd
from faker import Factory
import random
import testing.postgresql

from ratschlab_common.db.utils import PostgresDBParams

df_row_nr = 100

@pytest.fixture(scope='session')
def a_df():
    fake = Factory.create()
    fake.seed_instance(420)

    records = []
    for _ in range(df_row_nr):
        records.append([
            fake.name(),
            fake.email(),
            fake.date_time(),
            random.random(),
            random.randint(1, 20)
        ])

    col_names = ["col{}".format(i) for i in range(len(records[0]))]
    return pd.DataFrame.from_records(records, columns=col_names)


simple_db_table_name = 'mytable'
simple_db_partition_col = 'col4'  # integer column in 'a_df' fixture


@pytest.fixture(scope="module")
def simple_db(a_df):
    db = testing.postgresql.Postgresql()

    db_params = PostgresDBParams(port=db.settings['port'], db='test',
                                 password='test')

    conn = db_params.connection()
    a_df.to_sql(simple_db_table_name, conn, index=False)

    return db_params, db
