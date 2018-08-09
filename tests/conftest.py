import pytest
import pandas as pd
from faker import Factory
import random

# TODO refactor this out?
@pytest.fixture(scope='session')
def a_df():
    fake = Factory.create()
    fake.seed_instance(420)

    nrows = 100
    records = []
    for _ in range(nrows):
        records.append([
            fake.name(),
            fake.email(),
            fake.date_time(),
            random.random(),
            random.randint(1, 20)
        ])

    col_names = ["col{}".format(i) for i in range(len(records[0]))]
    return pd.DataFrame.from_records(records, columns=col_names)
