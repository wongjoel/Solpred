"""
Plot true vs prediction to visualise where the errors occur
"""
# Standard Library Modules
from pathlib import Path
from datetime import datetime
import math

# External Modules
import pandas as pd
import matplotlib.pyplot as plt
import sklearn.metrics as metrics


def scatter_inner(ax, df, actual, model, ax_title):
    ax.scatter(df[actual], df[model], 1)
    minval = df[[actual, model]].min().min()
    maxval = df[[actual, model]].max().max()
    ax.plot([minval, maxval], [minval, maxval], ls="--", color="black")
    ax.set_xlabel('Actual GHI (W/m2)')
    ax.set_ylabel('Forecast GHI (W/m2)')
    rmse = math.sqrt(metrics.mean_squared_error(df[actual], df[model]))
    ax.set_title(f"{ax_title} RMSE = {rmse:.2f}")
    return ax


def scatter_error(df, actual, model, ax_title="", fig_title="", filebase="plot", output_path=Path("")):
    fig, ax = plt.subplots()
    fig.set_size_inches(8, 8)
    ax = scatter_inner(ax, df, actual, model, ax_title)
    fig.suptitle(fig_title)
    fig.savefig(output_path)
    plt.close(fig)

def scatter_error_quad(df_list, actual, model, ax_title_list=[""], fig_title="", filebase="plot", output_path=Path("")):
    fig, ((ax0, ax1), (ax2, ax3)) = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(11, 11)
    ax0 = scatter_inner(ax0, df_list[0], actual, model, ax_title_list[0])
    ax1 = scatter_inner(ax1, df_list[1], actual, model, ax_title_list[1])
    ax2 = scatter_inner(ax2, df_list[2], actual, model, ax_title_list[2])
    ax3 = scatter_inner(ax3, df_list[3], actual, model, ax_title_list[3])
    fig.suptitle(fig_title)
    fig.savefig(output_path)
    plt.close(fig)

def read_parquet(path):
    df = pd.read_parquet(path)
    df["datetime"] = pd.to_datetime(df["predicted-time"], format="%Y-%m-%d_%H-%M-%S")
    df = df.set_index("datetime")
    df = df.sort_index()
    return df

def plot1():
    df = read_parquet("/results/CaseStudies/scatter/All/blackmountain_fully-conv_2x60s_120s_run_03_crop_1024_lr_3.0E-6_fold_4_metrics.parquet")
    df = df.dropna()
    scatter_error(
        df,
        actual="actual",
        model="fully-conv_2x60s_120s_run_03_fold_4",
        ax_title="",
        fig_title=f"Actual vs Forecast GHI. Category: All",
        output_path="/results/CaseStudies/scatter/scatter-all.pdf")

def plot_quad1():
    df_list = [
        read_parquet("/results/CaseStudies/scatter/All/blackmountain_fully-conv_2x60s_120s_run_03_crop_1024_lr_3.0E-6_fold_4_metrics.parquet").dropna(),
        read_parquet("/results/CaseStudies/scatter/Intermittent/blackmountain_fully-conv_2x60s_120s_run_03_crop_1024_lr_3.0E-6_fold_4_metrics.parquet").dropna(),
        read_parquet("/results/CaseStudies/scatter/Overcast/blackmountain_fully-conv_2x60s_120s_run_03_crop_1024_lr_3.0E-6_fold_4_metrics.parquet").dropna(),
        read_parquet("/results/CaseStudies/scatter/Sunny/blackmountain_fully-conv_2x60s_120s_run_03_crop_1024_lr_3.0E-6_fold_4_metrics.parquet").dropna(),
        ]
    scatter_error_quad(
        df_list,
        actual="actual",
        model="fully-conv_2x60s_120s_run_03_fold_4",
        ax_title_list=["All", "Intermittent", "Overcast", "Sunny"],
        fig_title=f"Solpred model, 2 inputs 60 seconds apart, 2 minute forecast horizon\nActual vs Forecast GHI by category",
        output_path="/results/CaseStudies/scatter/scatter-quad.png")



def main():
    plot1()
    plot_quad1()


if __name__ == '__main__':
    main()