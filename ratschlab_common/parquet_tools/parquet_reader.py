import pyarrow.parquet as pq
import pandas as pd


class ParquetReader:
    def __init__(self, path):

        self.pq_file = pq.ParquetFile(path)
        self.col_names = [i.name for i in self.pq_file.schema]
        self._max_len_col = max([len(name) for name in self.col_names])

    def head(self, n=5):
        for i, lines in enumerate(self._read(max_x=n)):
            self.pretty_print(lines, i)

    def cat(self):
        for i, lines in enumerate(self._read()):
            self.pretty_print(lines, i)

    def tail(self, n=5):
        for i, lines in enumerate(self._read_reverse(max_x=n)):
            self.pretty_print(lines, i)

    def pretty_print(self, lines, i, offset=3):
        if i == 0:
            print(" "*offset, end='')
            for col in self.col_names:
                print(col, end=" "*offset)
            print()

        for l, i in zip(lines.values, lines.index):
            print(" " + str(i), end=' '*(len(str(i)) + offset -1))
            for el, col in zip(l, self.col_names):
                print(" " + str(el), end=' '*(len(col) - len(str(el)) + offset -1))
            print()
        #print(lines.to_string(header=False, col_space=2))


    def _read(self, min_x=0, max_x=-1):

        l_c = 0
        for i in range(self.pq_file.num_row_groups):
            df = self.pq_file.read_row_group(i).to_pandas()
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

