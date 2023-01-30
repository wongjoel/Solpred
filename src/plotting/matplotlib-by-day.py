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

#Logging
import logging
import logging.config
import json

with open("logging_config.json") as f:
    log_config = json.load(f)
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


class WrongColumnError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

def line_timeseries(df, labels, ax_title="", fig_title="", filebase="plot", output_dir=Path("")):
    fig, ax = plt.subplots()
    fig.set_size_inches(25, 10)
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
    

def plot_date():
    print(f"Plotting from {left_file} {right_file} for date {date}")
    

def plot_dir(basedir="/work/processed-runs/blackmountain", outdir="/work"):
    basedir = Path(basedir)
    outdir = Path(outdir)
    outdir.mkdir(exist_ok=True)
    logger.debug(f"Plotting from {basedir.as_posix()}")
    
    for f in basedir.iterdir():
        stem = f.stem
        output_dir = outdir / stem
        output_dir.mkdir(exist_ok=True)
        logger.debug(f"Working with {stem}")
        df = read_parquet(f)
        df = df.drop(columns=["predicted-time", "id-time"])

        actual_label = df.columns[0]
        model_label = df.columns[1]
        persist_label = df.columns[2]

        if not "actual" in actual_label:
            raise WrongColumnError(f"{actual_label} is meant to be the actual column")

        if not "persist" in persist_label:
            raise WrongColumnError(f"{persist_label} is meant to be the persist column")

        dates = df["date"].unique()
        for date in dates:
            logger.debug(f"processing {date}")
            date_df = df[df["date"] == date]
            date_df = date_df.drop(columns=["date"])
            axis_title = "Axis Title"
            figure_title = "figure Title"
            file_base = f"{date}_{stem}"
            
            line_timeseries(date_df, [actual_label, model_label], axis_title, figure_title, file_base, output_dir)



def main():
    plot_dir("/work/processed-runs/blackmountain-round_2_blackmountain-ramp-round/wide-na", "/work/processed-runs/blackmountain-round_2_blackmountain-ramp-round/plots")


if __name__ == '__main__':
    main()