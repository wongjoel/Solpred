import datetime
from pathlib import Path
from multiprocessing import Pool

import numpy as np
import pandas as pd
import plotly_tools as plot_tools
import sklearn.metrics as metrics

#Logging
import logging
import logging.config
import json

with open("logging_config.json") as f:
    log_config = json.load(f)
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


def read_parquet(path):
    df = pd.read_parquet(path)
    df["datetime"] = pd.to_datetime(df["predicted-time"], format="%Y-%m-%d_%H-%M-%S")
    df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
    df = df.set_index("datetime")
    df = df.sort_index()
    df = df.drop(columns=["date", "id-time", "predicted-time"])
    cols=df.columns.tolist()
    cols.sort()
    df = df[cols]
    return df

def get_df(date, start, end):
    print(f"{date} {start} {end}")
    df = pd.read_parquet(f"/work/{date}.parquet")
    subset_df = df.between_time(start, end).copy() # Copy to get rid of view/copy ambiguity
    return subset_df

def custom_plot(df, date, suffix, channel):
    print("=============================================")
    out_dir = Path(f"/work/interact_CH{channel}")
    out_dir.mkdir(exist_ok=True)
    cols = [f"CH{ch}_V{num}" for num in range(1, 4) for ch in range(1, 7)]
    cols = cols + [f"CH{ch}_I{num}" for num in range(1, 4) for ch in range(1, 7) if ch != channel]
    df = df.drop(columns=cols)
    plot_tools.time_series(df, title=f"{date}_{suffix}", out_dir=out_dir)

def pool_func(args):
    date, start, end, suffix = args
    df = get_df(date, start, end)
    for ch in range(1, 7):
        custom_plot(df, date, suffix, ch)

def main():
    dt = datetime.datetime(2022, 7, 11, 8)
    date = dt.strftime("%Y-%m-%d")
    time_list = []
    while dt.time() < datetime.time(23,0):
        start = dt.time()
        dt = dt + datetime.timedelta(minutes=10)
        end = dt.time()
        time_list.append((date, start, end, str(start)))
        
    print("Starting pool")
    with Pool(processes=3, maxtasksperchild=1) as pool:
        pool.imap_unordered(pool_func, time_list)
        pool.close()
        pool.join()
    print("Done")

def custom_plot2(df, date, suffix):
    print("=============================================")
    out_dir = Path(f"/work/interact_CH26")
    out_dir.mkdir(exist_ok=True)
    cols = [f"CH{ch}_V{num}" for num in range(1, 4) for ch in range(1, 7)]
    cols = cols + [f"CH{ch}_I{num}" for num in range(1, 4) for ch in range(3, 6)]
    cols = cols + [f"CH{ch}_I{num}" for num in range(1, 4) for ch in range(1, 2)]
    df = df.drop(columns=cols)
    plot_tools.time_series(df, title=f"{date}_{suffix}", out_dir=out_dir)

def main2():
    dt = datetime.datetime(2022, 7, 11, 15)
    date = dt.strftime("%Y-%m-%d")
    start = dt.time()
    dt = dt + datetime.timedelta(minutes=10)
    end = dt.time()
    df = get_df(date, start, end)
    custom_plot2(df, date, str(start))

if __name__ == '__main__':
    main()