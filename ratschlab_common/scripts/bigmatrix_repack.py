import sys
import click
import subprocess
from ratschlab_common.io import bigmatrix
from pathlib import Path

DEFAULT_MAT_OPT = ['--keep-source-filters']
DEFAULT_ROW_OPT = ['--keep-source-filters']
DEFAULT_COL_OPT = ['--keep-source-filters']


def _call_ptrepack(src, dest, key, opts):
    args = ['ptrepack']
    args.extend(opts)
    args.extend(["{}:/{}".format(src, key),
                 "{}:/{}".format(dest, key)])

    subprocess.call(args)


@click.command()
@click.argument("source-file", type=click.Path(exists=True))
@click.argument("dest-file", type=click.Path(exists=False))
@click.option("--matrix-chunkshape", type=str,
              help='Chunkshape of the matrix. Can be a tuple like "(10, 20)" '
                   'or "keep" or "auto"')
@click.option("--matrix-repack-options", type=str,
              help='options passed to ptrepack for the matrix node. If set, '
                   'the matrix-chunkshape option is ignored.')
@click.option("--row-meta-repack-options", type=str,
              help='options passed to ptrepack for the row metadata node.')
@click.option("--col-meta-repack-options", type=str,
              help='options passed to ptrepack for the column metadata node.')
@click.option("--force", is_flag=True, default=False,
              help='If set, destination file will be overwritten if it exists')
def main(source_file, dest_file, matrix_chunkshape, matrix_repack_options,
         row_meta_repack_options, col_meta_repack_options, force):
    """
    Repacks bigmatrix h5 files using pytables' ptrepack.

    Main use case is to experiment with different chunkshapes for the matrix.
    """

    if Path(dest_file).exists():
        if force:
            Path(dest_file).unlink()
        else:
            raise(FileExistsError("File {} already exists!".format(dest_file)))

    if matrix_repack_options:
        matrix_opts = matrix_repack_options.split(' ')
    else:
        matrix_opts = DEFAULT_MAT_OPT.copy()

        if matrix_chunkshape:
            matrix_opts.extend(['--chunkshape', matrix_chunkshape])
    _call_ptrepack(source_file, dest_file, bigmatrix.DATA_KEY,
                         matrix_opts)

    row_opts = row_meta_repack_options.split(' ') if row_meta_repack_options else \
        DEFAULT_ROW_OPT
    _call_ptrepack(source_file, dest_file, bigmatrix.ROW_DESC_KEY,
                        row_opts)

    col_opts = col_meta_repack_options.split(' ') if col_meta_repack_options \
        else \
        DEFAULT_COL_OPT
    _call_ptrepack(source_file, dest_file, bigmatrix.COL_DESC_KEY,
                        col_opts)

    return 0


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    sys.exit(main())
