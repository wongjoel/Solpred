"""
Plot single day with metrics
"""
# Standard Library Modules
from pathlib import Path
from datetime import datetime
import math

# External Modules
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sklearn.metrics as metrics

def line_timeseries(df, labels, ax_title="", fig_title="", filebase="plot", output_dir=Path("")):
    fig, ax = plt.subplots()
    fig.set_size_inches(10, 4)
    fmt_string = '.-'
    for label in labels:
        ax.plot(df.index, df[label], fmt_string, label=label)
    plt.xlabel('Timestep')
    plt.ylabel('GHI (W/m2)')
    plt.legend(loc="lower left")
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
    return df

def plot_common(x, date, figure_title, short_descriptor):
    actual_label = x.columns[0]
    model_label = x.columns[1]
    persist_label = x.columns[2]

    # Plot actual
    axis_title = f"Actual irradiance"
    file_base = f"{date}_{short_descriptor}_actual"
    output_dir = Path("./")
    line_timeseries(x, [actual_label], axis_title, figure_title, file_base, output_dir)

    # Plot context
    axis_title = "Context (full chart)"
    file_base = f"{date}_{short_descriptor}_context"
    line_timeseries(x, [actual_label, model_label, persist_label], axis_title, figure_title, file_base, output_dir)

    # Plot input vs output
    axis_title = "Input irradiance + prediction/actual"
    file_base = f"{date}_{short_descriptor}_input"
    x.iloc[0:22, 1:3] = np.NaN  # Blank out previous predictions
    x.iloc[16:22, 0] = np.NaN   # Blank out intermiediate actuals
    line_timeseries(x, [actual_label, model_label, persist_label], axis_title, figure_title, file_base, output_dir)

def plot_sample1():
    date = "2015-01-31"
    df = read_parquet("/work/processed-runs/blackmountain-round_2_blackmountain-ramp-round/wide-na/blackmountain-round_2_blackmountain-ramp-round_fully-conv_16x60s_420s_run_00_lr_3.0E-6_fold_0_wide-na.parquet")
    date_df = df[df["date"] == date]
    date_df = date_df.drop(columns=["date", "predicted-time", "id-time"])
    x = date_df.between_time("12:16", "12:39").copy() # Copy to get rid of view/copy ambiguity
    
    short_descriptor = "bmr2bmrr"
    figure_title = f"Trained on round, then finetuned on just ramps {date} {short_descriptor}"
    plot_common(x, date, figure_title, short_descriptor)

    # -------------- #
    df = read_parquet("/work/processed-runs/blackmountain-round/wide-na/blackmountain-round_fully-conv_16x60s_420s_run_00_lr_3.0E-6_fold_0_wide-na.parquet")
    date_df = df[df["date"] == date]
    date_df = date_df.drop(columns=["date", "predicted-time", "id-time"])
    x = date_df.between_time("12:16", "12:39").copy() # Copy to get rid of view/copy ambiguity

    short_descriptor = "bmr"
    figure_title = f"trained on round only {date} {short_descriptor}"
    plot_common(x, date, figure_title, short_descriptor)

    # -------------- #
    df = read_parquet("/work/processed-runs/blackmountain-ramp-round/wide-na/blackmountain-ramp-round_fully-conv_16x60s_420s_run_00_lr_3.0E-6_fold_0_wide-na.parquet")
    date_df = df[df["date"] == date]
    date_df = date_df.drop(columns=["date", "predicted-time", "id-time"])
    x = date_df.between_time("12:16", "12:39").copy() # Copy to get rid of view/copy ambiguity

    short_descriptor = "bmrr"
    figure_title = f"trained on ramps only {date} {short_descriptor}"
    plot_common(x, date, figure_title, short_descriptor)


def plot_sample2():
    date = "2015-02-05"
    df = read_parquet("/work/processed-runs/blackmountain-round_2_blackmountain-ramp-round/wide-na/blackmountain-round_2_blackmountain-ramp-round_fully-conv_16x60s_420s_run_00_lr_3.0E-6_fold_0_wide-na.parquet")
    date_df = df[df["date"] == date]
    date_df = date_df.drop(columns=["date", "predicted-time", "id-time"])
    x = date_df.between_time("12:31", "12:54").copy() # Copy to get rid of view/copy ambiguity

    short_descriptor = "bmr2bmrr"
    figure_title = f"Trained on round, then finetuned on just ramps {date} {short_descriptor}"
    plot_common(x, date, figure_title, short_descriptor)

    # -------------- #
    df = read_parquet("/work/processed-runs/blackmountain-round/wide-na/blackmountain-round_fully-conv_16x60s_420s_run_00_lr_3.0E-6_fold_0_wide-na.parquet")
    date_df = df[df["date"] == date]
    date_df = date_df.drop(columns=["date", "predicted-time", "id-time"])
    x = date_df.between_time("12:31", "12:54").copy() # Copy to get rid of view/copy ambiguity

    short_descriptor = "bmr"
    figure_title = f"trained on round only {date} {short_descriptor}"
    plot_common(x, date, figure_title, short_descriptor)

    # -------------- #
    df = read_parquet("/work/processed-runs/blackmountain-ramp-round/wide-na/blackmountain-ramp-round_fully-conv_16x60s_420s_run_00_lr_3.0E-6_fold_0_wide-na.parquet")
    date_df = df[df["date"] == date]
    date_df = date_df.drop(columns=["date", "predicted-time", "id-time"])
    x = date_df.between_time("12:31", "12:54").copy() # Copy to get rid of view/copy ambiguity

    short_descriptor = "bmrr"
    figure_title = f"trained on ramps only {date} {short_descriptor}"
    plot_common(x, date, figure_title, short_descriptor)


def main():
    plot_sample1()
    plot_sample2()


if __name__ == '__main__':
    main()