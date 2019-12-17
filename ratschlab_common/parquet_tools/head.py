import click
from ratschlab_common.parquet_tools.parquet_reader import ParquetReader
import sys

@click.command()
@click.argument('filepath', type=click.Path(exists=True))
@click.option('--n', type=click.INT, default=5)
def main(filepath, n):
    reader = ParquetReader(filepath)
    reader.head(n=n)

if __name__ == '__main__':
    sys.exit(main())
