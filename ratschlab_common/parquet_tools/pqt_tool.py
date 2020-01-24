from ratschlab_common.parquet_tools.parquet_reader import ParquetReader
import sys
import click

'''
Examples: 
 python ratschlab_common/parquet_tools/pqt_tool.py tail path_to_file --n 7
 python ratschlab_common/parquet_tools/pqt_tool.py cat path_to_file
 python ratschlab_common/parquet_tools/pqt_tool.py head path_to_file --n 5 -H
 python ratschlab_common/parquet_tools/pqt_tool.py schema path_to_file 
'''

@click.group()
def pqt_tool():
    pass

@pqt_tool.command()
@click.argument('filepath', type=click.Path(exists=True))
@click.option('--n', type=click.INT, default=5)
@click.option('-H', is_flag=True)
def tail(filepath, n, h):
    reader = ParquetReader(filepath)
    reader.tail(n=n, header=h)

@pqt_tool.command()
@click.argument('filepath', type=click.Path(exists=True))
@click.option('--n', type=click.INT, default=5)
@click.option('-H', is_flag=True)
def head(filepath, n, h):
    reader = ParquetReader(filepath)
    reader.head(n=n, header=h)

@pqt_tool.command()
@click.argument('filepath', type=click.Path(exists=True))
@click.option('-H', is_flag=True)
def cat(filepath, h):
    reader = ParquetReader(filepath)
    reader.cat(header=h)

@pqt_tool.command()
@click.argument('filepath', type=click.Path(exists=True))
def schema(filepath):
    reader = ParquetReader(filepath)
    reader.schema()

if __name__ == '__main__':
    sys.exit(pqt_tool())




