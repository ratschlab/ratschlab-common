import click
from ratschlab_common.parquet_tools.parquet_reader import ParquetReader
import sys

'''
Examples: 
 python ratschlab_common/parquet_tools/cat.py path_to_file
'''

@click.command()
@click.argument('filepath', type=click.Path(exists=True))
def main(filepath):
    reader = ParquetReader(filepath)
    reader.cat()

if __name__ == '__main__':
    sys.exit(main())
