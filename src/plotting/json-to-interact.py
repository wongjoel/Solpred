from typing import Union
from datetime import datetime
from pathlib import Path
import math
import argparse
import json

import numpy as np
import pandas as pd
import plotly_tools as plot_tools
import sklearn.metrics as metrics


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
    plot_tools.time_series(read_parquet(instr["path"]), title=instr["title"], out_dir=instr["out-dir"], intervals=['_upper', '_lower'])


if __name__ == '__main__':
    main(parse_args())
