import click
from ratschlab_common.parquet_tools.parquet_reader import ParquetReader
import sys
'''
Examples: 
 python ratschlab_common/parquet_tools/tail.py path_to_file --n 7
 python ratschlab_common/parquet_tools/tail.py path_to_file
'''

@click.command()
@click.argument('filepath', type=click.Path(exists=True))
@click.option('--n', type=click.INT, default=5)
def main(filepath, n):
    reader = ParquetReader(filepath)
    reader.tail(n=n)

if __name__ == '__main__':
    sys.exit(main())
