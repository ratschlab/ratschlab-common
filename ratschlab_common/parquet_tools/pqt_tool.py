from ratschlab_common.parquet_tools.parquet_reader import ParquetReader
import sys
import click

'''
Examples: 
 python ratschlab_common/parquet_tools/pqt_tool.py tail path_to_file --n 7
 python ratschlab_common/parquet_tools/pqt_tool.py cat path_to_file
 python ratschlab_common/parquet_tools/pqt_tool.py path_to_file --n 5
'''

@click.group()
def pqt_tool():
    print('hello')

@pqt_tool.command()
@click.argument('filepath', type=click.Path(exists=True))
@click.option('--n', type=click.INT, default=5)
def tail(filepath, n):
    reader = ParquetReader(filepath)
    reader.tail(n=n)

@pqt_tool.command()
@click.argument('filepath', type=click.Path(exists=True))
@click.option('--n', type=click.INT, default=5)
def head(filepath, n):
    reader = ParquetReader(filepath)
    reader.head(n=n)

@pqt_tool.command()
@click.argument('filepath', type=click.Path(exists=True))
def cat(filepath):
    reader = ParquetReader(filepath)
    reader.cat()

if __name__ == '__main__':
    sys.exit(pqt_tool())




