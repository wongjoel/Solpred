"""
Plot single day with metrics
"""
# Standard Library Modules
from pathlib import Path
from datetime import datetime
import math

# External Modules
import pandas as pd
import matplotlib.pyplot as plt
import sklearn.metrics as metrics

def line_timeseries(timeline, actual, left_model, left_label, right_model, right_label, ax_title="", fig_title="", filebase="plot", output_dir=Path("")):
    fig, ax = plt.subplots()
    fig.set_size_inches(30, 10)
    ax.plot(timeline, left_model, label=left_label)
    ax.plot(timeline, right_model, label=right_label)
    ax.plot(timeline, actual, label="actual")
    plt.xlabel('Timestep')
    plt.ylabel('GHI (W/m2)')
    plt.legend(loc="upper left")
    ax.set_title(ax_title)
    fig.suptitle(fig_title)
    fig.savefig(output_dir / f"{filebase}.png")
    plt.close(fig)

def read_df_date(path, date):
    dt_parser = lambda x: datetime.strptime(x, "%Y-%m-%d_%H-%M-%S")
    df = pd.read_csv(path, parse_dates=["time"], date_parser=dt_parser)
    df["date"] = df["time"].dt.strftime("%Y-%m-%d")
    df = df.set_index("time")
    df = df[df["date"] == date]
    df = df.drop(columns=["date"])
    return df
    

def plot_day(left_file, right_file, actual="actual", date="2015-06-19", left_model="fully-conv_16x60s_420s_run_01", right_model="sunset_16x60s_7m_run_00"):
    print(f"Plotting from {left_file} {right_file} for date {date}")
    
    df1 = read_df_date(left_file, date)
    df2 = read_df_date(right_file, date)

    df = df1.join(df2, rsuffix="right", sort=True)
    rmse_left = math.sqrt(metrics.mean_squared_error(df[actual], df[left_model]))
    rmse_right = math.sqrt(metrics.mean_squared_error(df[actual], df[right_model]))

    line_timeseries(df.index, df[actual], df[left_model], left_model, df[right_model], right_model, ax_title=f"MyConv RMSE: {rmse_left:.1f} Sunset RMSE: {rmse_right:.1f}", fig_title=f"GHI vs Time for {date} at BlackMountain", filebase=date)

def plot_dir(basedir="/work/processed-runs/blackmountain", outdir="/work", title_prefix="bm_"):
    basedir = Path(basedir)
    outdir = Path(outdir)
    outdir.mkdir(exist_ok=True)
    print(f"Plotting from {basedir.as_posix()}")
    
    for f in basedir.iterdir():
        print(f.stem)
        df = pd.read_parquet(f)
        df["datetime"] = pd.to_datetime(df["time"], format="%Y-%m-%d_%H-%M-%S")
        df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
        df = df.set_index("datetime")
        df = df.sort_index()
        df = df.drop(columns='time')
        grouped = df.groupby("date")

        for name, group in grouped:
            print(name)
            print(group)
        breakpoint()




def main():
    plot_dir("/work/processed-runs/blackmountain-round_2_blackmountain-ramp-round/wide", "/work/processed-runs/blackmountain-round_2_blackmountain-ramp-round/plots", "bmr-2-bmrr_")
    # plot_day(left_file="/work/processed-runs/blackmountain-round/wide/16x60s_420s_fully-conv_run_00_fold_0_wide.csv.gz", 
    #         right_file="/work/processed-runs/blackmountain-round/wide/16x60s_420s_sunset_run_00_fold_0_wide.csv.gz",
    #         actual="actual",
    #         date="2015-06-19",
    #         left_model="fully-conv_16x60s_420s_run_00_fold_0",
    #         right_model="sunset_16x60s_420s_run_00_fold_0")


if __name__ == '__main__':
    main()