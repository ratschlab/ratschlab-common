from ratschlab_common.parquet_tools.parquet_reader import ParquetReader
import sys
import click

'''
Examples: 
 pqt_tool tail path_to_file --n 7
 pqt_tool cat path_to_file
 pqt_tool head path_to_file --n 5 -H
 pqt_tool schema path_to_file 
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




