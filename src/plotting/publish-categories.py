"""
Plot single day with metrics
"""
# Standard Library Modules
from pathlib import Path
import datetime
import math
import datetime

# External Modules
import pandas as pd
import matplotlib
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


def solpred_line_timeseries(df, actual, ax_title="", fig_title="", xlabel='Timestep', ylabel="", y_top=1000, y_bot=0, output_path=Path("")):
    fig, ax = plt.subplots()
    fig.set_size_inches(6, 4)
    ax.plot(df.index, df[actual], '-', label="Actual")
    ax.set_ylim(bottom=y_bot, top=y_top, emit=True, auto=False)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    # plt.legend()
    ax.set_title(f"{ax_title}")
    fig.suptitle(f"{fig_title}")
    fig.savefig(output_path)
    plt.close(fig)


def read_parquet(path):
    df = pd.read_parquet(path)
    df["datetime"] = pd.to_datetime(df["predicted-time"], format="%Y-%m-%d_%H-%M-%S")
    df = df.set_index("datetime")
    df = df.sort_index()
    return df

def filter_date(df, start_time, end_time):
    df = df[df.index < end_time]
    df = df[df.index > start_time]
    df = df.dropna()
    return df

def my_func_123():
    basefolder="/work/processed-runs/blackmountain/categories/All/wide"
    model="fully-conv"
    run="run_06"
    inout="2x60s_300s"
    crop="crop_1024"
    lr="lr_3.0E-4"
    fold="fold_3"
    y_top=1200
    y_bot=0
    

    df = read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet")
    start_time = datetime.datetime(2015, 7, 20)
    end_time = datetime.datetime(2015, 7, 21)
    df = filter_date(df, start_time, end_time)

    solpred_line_timeseries(
        df,
        actual="actual",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Example Sunny Day: 2015-07-20",
        fig_title="",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"/work/plots/sunny-2015-07-20.pdf"
    )

    df = read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet")
    start_time = datetime.datetime(2015, 3, 21)
    end_time = datetime.datetime(2015, 3, 22)
    df = filter_date(df, start_time, end_time)

    solpred_line_timeseries(
        df,
        actual="actual",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Example Intermittent Day: 2015-03-21",
        fig_title="",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"/work/plots/intermittent-2015-03-21.pdf"
    )
    
    df = read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet")
    start_time = datetime.datetime(2015, 7, 15)
    end_time = datetime.datetime(2015, 7, 16)
    df = filter_date(df, start_time, end_time)

    solpred_line_timeseries(
        df,
        actual="actual",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Example Overcast Day: 2015-07-15",
        fig_title="",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"/work/plots/overcast-2015-07-15.pdf"
    )

    df = read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet")
    start_time = datetime.datetime(2015, 9, 18)
    end_time = datetime.datetime(2015, 9, 19)
    df = filter_date(df, start_time, end_time)

    solpred_line_timeseries(
        df,
        actual="actual",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Example Mixed Day: 2015-09-18",
        fig_title="",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"/work/plots/mixed-2015-09-18.pdf"
    )
    

def main():
    my_func_123()


if __name__ == '__main__':
    main()