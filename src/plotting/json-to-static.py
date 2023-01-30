"""
Plot single day with metrics
"""
# Standard Library Modules
from pathlib import Path
from datetime import datetime
import math
import argparse
import json

# External Modules
import pandas as pd
import matplotlib.pyplot as plt
import sklearn.metrics as metrics


def line_timeseries(df, labels=None, ax_title="", fig_title="", filebase="plot", out_dir=Path("")):
    fig, ax = plt.subplots()
    fig.set_size_inches(25, 10)
    if not labels:
        labels = df.columns
    for label in labels:
        ax.plot(df.index, df[label], '.-', label=label)
    plt.xlabel('Timestep')
    plt.ylabel('GHI (W/m2)')
    plt.legend(loc="lower left")
    ax.set_title(ax_title)
    fig.suptitle(fig_title)
    fig.savefig(Path(out_dir) / f"{filebase}.png")
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


def parse_args():
    parser = argparse.ArgumentParser(description="CLI script to crop and resize images")
    parser.add_argument('instr_file', type=Path, help="Json file with instructions")
    return parser.parse_args()


def main(args):
    with open(args.instr_file) as f:
        instr = json.load(f)
    line_timeseries(read_parquet(instr["path"]), fig_title=instr["title"], filebase=instr["title"], out_dir=instr["out-dir"])

if __name__ == '__main__':
    main(parse_args())
