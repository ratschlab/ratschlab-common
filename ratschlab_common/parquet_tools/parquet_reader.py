import pyarrow.parquet as pq
import pandas as pd
import time


class ParquetReader:
    def __init__(self, path, batch_size=10000):

        self.pq_file = pq.ParquetFile(path)
        self.batches = self.pq_file.reader.read_all().to_batches(batch_size)
        self.col_names = [i.name for i in self.pq_file.schema]
        self.col_types = [i.physical_type for i in self.pq_file.schema]
        self._max_len_col = max([len(name) for name in self.col_names])

    def head(self, n=5, header=True):
        if header:
            self._pretty_print_header()
        for i, lines in enumerate(self._read(max_x=n)):
            self.pretty_print(lines)

    def cat(self, header=True):
        if header:
            self._pretty_print_header()
        for i, lines in enumerate(self._read()):
            self.pretty_print(lines)

    def tail(self, n=5, header=True):
        if header:
            self._pretty_print_header()
        for i, lines in enumerate(self._read_reverse(max_x=n)):
            self.pretty_print(lines)

    def schema(self):
        for col, t in zip(self.col_names, self.col_types):
            print(col + ': ' + t)
        print()

    def _pretty_print_header(self):
        print(" ", end="\t")
        for col in self.col_names:
            print(col, end="\t")
        print()

    def pretty_print(self, lines):

        for l, i in zip(lines.values, lines.index):
            print(" " + str(i), end='\t')
            for el, col in zip(l, self.col_names):
                print(" " + str(el), end='\t')
            print()
        #print(lines.to_string(header=False, col_space=2))


    def _read(self, min_x=0, max_x=-1):

        l_c = 0
        for batch in self.batches:
            df = batch.to_pandas()
            if max_x == -1:
                lines = df.iloc[max([min_x - l_c, 0]):]
            else:
                lines = df.iloc[max([min_x - l_c, 0]): max_x - l_c]
            l_c += len(lines)
            yield lines
            if l_c >= max_x > -1:
                break

    def _read_reverse(self, max_x=-1):

        l_c = 0
        for i in reversed(range(self.pq_file.num_row_groups)):
            df = self.pq_file.read_row_group(i).to_pandas()
            if max_x == -1:
                lines = df.iloc[:]
            else:
                lines = df.iloc[-max([max_x - l_c, 0]):]
            l_c += len(lines)
            # maybe not the most efficient here
            yield lines[::-1]
            if l_c >= max_x > -1:
                break




if __name__ == '''__main__''':
    path = "file.parquet"
    reader = ParquetReader(path)
    reader.head(n=7)
    reader.cat()
    reader.tail()
    reader.tail(n=8)

