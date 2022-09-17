import os
import pandas as pd
from julia import Main as Jl
from pathlib import Path

fp = Path(__file__).resolve().parents[0]

Jl.eval(f'''import TsDb: Client''')


def upsert(meta: dict, data):
    if isinstance(data, pd.Series):
        Jl.Client.py_upsert(meta, data.index.values, data.values)
    elif isinstance(data, pd.DataFrame):
        for col, data in data.iteritems():
            Jl.Client.py_upsert({**meta, **{'col': col}}, data.index.values, data.values)
    else:
        raise ValueError("Type of df not clear")


def query(meta: dict, start="", stop="9") -> pd.DataFrame:
    cols, mat = Jl.Client.py_query(meta, start=str(start), stop=str(stop))
    df = pd.DataFrame(mat, columns=cols)
    if 'ts' in cols:
        df = df.set_index("ts")
    print('Query Done.')
    return df


def matching_metas(meta: dict) -> [dict]:
    return Jl.Client.matching_metas(meta)


# if __name__ == '__main__':
#     import datetime
#     import pandas as pd
    # upsert({'a': 3}, pd.Series([1], index=[datetime.datetime(2022, 1, 1)]))
    # print(query(meta={
    #     "measurement_name" : "trade bars",
    #     "exchange" : "bitfinex",
    #     "asset" : "ethusd",
    #     "information" : "volume"
    # }))
    # Jl.Client.drop({
    #         "measurement_name": "trade bars",
    #         "exchange": 'bitfinex',
    #         "asset": 'ethusd',
    #         "information": 'price',
    #         "unit": "ethusd",
    #         "col": "price",
    #         "unit_size": 1,
    #     })
