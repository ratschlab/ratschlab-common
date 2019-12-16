from pathlib import Path

import pandas as pd
from click.testing import CliRunner

from ratschlab_common.scripts import export_db_to_files
from tests.db import data_fixtures


def test_export_db_to_files_base(simple_db, tmpdir):
    db_params, db = simple_db

    dest_path = Path(tmpdir, 'dump_dest')

    runner = CliRunner()

    result = runner.invoke(export_db_to_files.main,
                           ['--db-port', db_params.port,
                            '--db-username', db_params.user,
                            '--db-password', db_params.password,
                            '--db-name', db_params.db,
                            str(dest_path)])

    assert result.exit_code == 0

    df = pd.read_parquet(Path(dest_path, data_fixtures.simple_db_table_name))
    assert df.shape[0] == data_fixtures.df_row_nr

