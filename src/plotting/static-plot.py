"""
Plot single day with metrics
"""
# Standard Library Modules
from pathlib import Path
import datetime
from multiprocessing import Pool

# External Modules
import pandas as pd
import matplotlib.pyplot as plt
import sklearn.metrics as metrics

#Logging
import logging
import logging.config
import json

with open("logging_config.json") as f:
    log_config = json.load(f)
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


def line_timeseries(df, labels, ax_title="", fig_title="", xlabel='Timestep', ylabel="", filebase="plot", output_dir=Path("")):
    fig, ax = plt.subplots()
    fig.set_size_inches(25, 10)
    fmt_string = '.-'
    for label in labels:
        ax.plot(df.index, df[label], fmt_string, label=label)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    ax.set_title(ax_title)
    fig.suptitle(fig_title)
    fig.savefig(output_dir / f"{filebase}.png")
    plt.close(fig)


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

def custom_plot(df, date, suffix):
    print("=============================================")
    for ch in range(1, 7):
        out_dir = Path(f"/work/Static_CH{ch}")
        out_dir.mkdir(exist_ok=True)
        line_timeseries(df, [f"CH{ch}_I1", f"CH{ch}_I2", f"CH{ch}_I3"], fig_title=date, filebase=f"{date}_{suffix}", output_dir=out_dir)

def pool_func(args):
    date, start, end, suffix = args
    df = get_df(date, start, end)
    custom_plot(df, date, suffix)

def main():
    dt = datetime.datetime(2022, 7, 12, 0)
    date = dt.strftime("%Y-%m-%d")
    time_list = []
    while dt.time() < datetime.time(23,0):
        start = dt.time()
        dt = dt + datetime.timedelta(minutes=15)
        end = dt.time()
        time_list.append((date, start, end, str(start)))
        
    print("Starting pool")
    with Pool(processes=3, maxtasksperchild=1) as pool:
        pool.imap_unordered(pool_func, time_list)
        pool.close()
        pool.join()
    print("Done")
        


if __name__ == '__main__':
    main()