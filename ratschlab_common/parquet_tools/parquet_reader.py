import pyarrow.parquet as pq
import pandas as pd

path = "file.parquet"

class ParquetReader:
    def __init__(self, path):

        self.pq_file = pq.ParquetFile(path)
        self.col_names = [i.name for i in self.pq_file.schema]
        self._max_len_col = max([len(name) for name in self.col_names])
        #self.table = pq.read_pandas(path)

    def head(self, n=5):
        for i, lines in enumerate(self._read(max_x=n)):
            self.pretty_print(lines, i)

    def cat(self):
        for i, lines in enumerate(self._read()):
            self.pretty_print(lines, i)

    def tail(self):
        pass

    def pretty_print(self, lines, i, offset=3):
        if i == 0:
            for col in self.col_names:
                print(col, end=" "*(self._max_len_col - len(col) + offset))
            print()

        for l in lines.values:
            for el in l:
                print(" " + str(el), end=' '*(self._max_len_col - len(str(el)) + offset -1))
            print()
        #print(lines.to_string(header=False, col_space=2))


    def _read(self, min_x=0, max_x=-1):

        l_c = 0
        ret_df = []
        for i in range(self.pq_file.num_row_groups):
            df = self.pq_file.read_row_group(i).to_pandas()
            if max_x == -1:
                lines = df.iloc[max([min_x - l_c, 0]):]
            else:
                lines = df.iloc[max([min_x - l_c, 0]): max_x - l_c]
            ret_df.append(lines)
            l_c += len(lines)
            yield lines
            if l_c >= max_x > -1:
                break




if __name__ == '''__main__''':
    reader = ParquetReader(path)
    lines = reader.head(n=7)
    lines = reader.cat()

