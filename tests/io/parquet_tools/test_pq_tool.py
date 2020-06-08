import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from ratschlab_common.io.parquet_tools.parquet_reader import ParquetReader
from contextlib import redirect_stdout
import io

name = "file.parquet"
dim_0 = 12
dim_1 = 4

@pytest.fixture
def df_test(tmp_path):
    # generate df
    df = pd.DataFrame(np.random.rand(dim_0, dim_1), columns=['A', 'B', 'C', 'D'])
    table = pa.Table.from_pandas(df)
    pq.write_table(table, tmp_path / name)
    return df

def parse_stdout(output, row_dim, col_dim):
    output = output.strip().split('\t')
    columns = output[:4]
    values = output[4:]
    values = [float(v.strip()) for v in values]
    values = np.array(values).reshape((row_dim,col_dim))
    d = pd.DataFrame(values, columns=columns)
    return d

@pytest.fixture(params=[4,5,6])
def reader(tmp_path, request):
    return ParquetReader(tmp_path / name, batch_size=request.param)

def test_head(df_test, reader):
    with io.StringIO() as buf, redirect_stdout(buf):
        reader.head(n=5)
        output = buf.getvalue()
    d = parse_stdout(output, row_dim=5, col_dim=dim_1)
    assert np.array_equal(d.values, df_test.head().values)

def test_cat(df_test, reader):
    with io.StringIO() as buf, redirect_stdout(buf):
        reader.cat()
        output = buf.getvalue()
    d = parse_stdout(output, row_dim=dim_0, col_dim=dim_1)
    assert np.array_equal(d.values, df_test.values)

def test_tail(df_test, reader):
    with io.StringIO() as buf, redirect_stdout(buf):
        reader.tail(n=5)
        output = buf.getvalue()
    d = parse_stdout(output, row_dim=5, col_dim=dim_1)
    assert np.array_equal(d.values, df_test.values[-5:, :])
