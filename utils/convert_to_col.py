import pandas
import os


def load_data(file: str):
    for sep in [',', '|', '\t']:
        df = None
        try:
            with open(file) as f:
                df = pandas.read_csv(f, sep=sep)
                return df
        except Exception as e:
            exception = e
            print(e)

# generate column files with conversion: {table name}-{column name}.csv
def generate_column_files(df: pandas.DataFrame, fname: str):
    for column in df: 
        print('Colunm name:', column)
        data = df[column]
        output_name = fname + "-" + column + ".csv"
        data.to_csv(output_name, header=False, index=False)


def load_and_generate():
    path = os.path.join("..", "col_data")
    for fname in os.listdir(path):
        print(fname)
        if fname.endswith(".tbl"):
            fpath = os.path.join(path, fname)
            df = load_data(fpath)
            table = fname.rstrip(".tbl")
            generate_column_files(df, table)


if __name__ == "__main__":
    load_and_generate()