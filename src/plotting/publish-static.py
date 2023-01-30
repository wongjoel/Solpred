"""
Plot single day with metrics
"""
# Standard Library Modules
from pathlib import Path
import datetime
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


def solpred_line_timeseries(df, actual, model, persist, ax_title="", fig_title="", xlabel='Timestep', ylabel="", y_top=1000, y_bot=0, output_path=Path("")):
    fig, ax = plt.subplots()
    fig.set_size_inches(9, 4)
    ax.plot(df.index, df[actual], '-', label="Actual")
    ax.plot(df.index, df[model], '--', label="Solpred")
    ax.plot(df.index, df[persist], ':', label="Persistence")
    ax.set_ylim(bottom=y_bot, top=y_top, emit=True, auto=False)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    r2 = metrics.r2_score(df[actual], df[model])
    rmse = math.sqrt(metrics.mean_squared_error(df[actual], df[model]))
    rmse_p = math.sqrt(metrics.mean_squared_error(df[actual], df[persist]))
    skill = (1 - rmse/rmse_p) * 100
    ax.set_title(f"{ax_title} RMSE$={rmse:.2f}$, Skill$={skill:.1f}\%$, $R^2={r2:.2f}$")
    fig.suptitle(f"{fig_title}")
    fig.savefig(output_path)
    plt.close(fig)


def read_parquet(path):
    df = pd.read_parquet(path)
    df["datetime"] = pd.to_datetime(df["predicted-time"], format="%Y-%m-%d_%H-%M-%S")
    df = df.set_index("datetime")
    df = df.sort_index()
    return df

def dec19():
    basefolder="/results/CaseStudies/19-dec-high"
    model="fully-conv"
    y_top=1075
    y_bot=925

    run="run_06"
    inout="2x60s_300s"
    crop="crop_1024"
    lr="lr_3.0E-4"
    fold="fold_3"
    solpred_line_timeseries(
        read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet"),
        actual="actual",
        model=f"{model}_{inout}_{run}_{fold}",
        persist=f"persist_{inout}_{run}_{fold}",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Solpred 5 minutes ahead.",
        fig_title="December 19 - High Cloud Band",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"{basefolder}/19-dec-{model}_{inout}_{run}_{fold}.pdf"
    )

    run="run_02"
    inout="16x120s_420s"
    crop="crop_full"
    lr="lr_3.0E-6"
    fold="fold_1"
    solpred_line_timeseries(
        read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet"),
        actual="actual",
        model=f"{model}_{inout}_{run}_{fold}",
        persist=f"persist_{inout}_{run}_{fold}",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Solpred 7 minutes ahead.",
        fig_title="December 19 - High Cloud Band",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"{basefolder}/19-dec-{model}_{inout}_{run}_{fold}.pdf"
    )

    run="run_03"
    inout="8x30s_120s"
    crop="crop_1024"
    lr="lr_3.0E-6"
    fold="fold_9"
    solpred_line_timeseries(
        read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet"),
        actual="actual",
        model=f"{model}_{inout}_{run}_{fold}",
        persist=f"persist_{inout}_{run}_{fold}",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Solpred 2 minutes ahead.",
        fig_title="December 19 - High Cloud Band",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"{basefolder}/19-dec-{model}_{inout}_{run}_{fold}.pdf"
    )

def apr25():
    basefolder="/results/CaseStudies/25-apr-switch"
    model="fully-conv"
    y_top=800
    y_bot=0

    run="run_06"
    inout="2x60s_120s"
    crop="crop_1024"
    lr="lr_3.0E-5"
    fold="fold_0"
    solpred_line_timeseries(
        read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet"),
        actual="actual",
        model=f"{model}_{inout}_{run}_{fold}",
        persist=f"persist_{inout}_{run}_{fold}",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Solpred 2 minutes ahead.",
        fig_title="April 25 - Sunny to Overcast",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"{basefolder}/25-apr-{model}_{inout}_{run}_{fold}.pdf"
    )

    run="run_06"
    inout="2x60s_300s"
    crop="crop_1024"
    lr="lr_3.0E-5"
    fold="fold_1"
    solpred_line_timeseries(
        read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet"),
        actual="actual",
        model=f"{model}_{inout}_{run}_{fold}",
        persist=f"persist_{inout}_{run}_{fold}",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Solpred 5 minutes ahead.",
        fig_title="April 25 - Sunny to Overcast",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"{basefolder}/25-apr-{model}_{inout}_{run}_{fold}.pdf"
    )

    run="run_02"
    inout="2x60s_420s"
    crop="crop_full"
    lr="lr_3.0E-6"
    fold="fold_1"
    solpred_line_timeseries(
        read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet"),
        actual="actual",
        model=f"{model}_{inout}_{run}_{fold}",
        persist=f"persist_{inout}_{run}_{fold}",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="Solpred 7 minutes ahead.",
        fig_title="April 25 - Sunny to Overcast",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"{basefolder}/25-apr-{model}_{inout}_{run}_{fold}.pdf"
    )

    model="sunset"

    run="run_02"
    inout="2x60s_120s"
    crop="crop_1024"
    lr="lr_3.0E-6"
    fold="fold_3"
    solpred_line_timeseries(
        read_parquet(f"{basefolder}/blackmountain_{model}_{inout}_{run}_{crop}_{lr}_{fold}_metrics.parquet"),
        actual="actual",
        model=f"{model}_{inout}_{run}_{fold}",
        persist=f"persist_{inout}_{run}_{fold}",
        xlabel="Time",
        ylabel="Irradiance ($W/m^2$)",
        ax_title="SUNSET* 2 minutes ahead.",
        fig_title="April 25 - Sunny to Overcast",
        y_top=y_top,
        y_bot=y_bot,
        output_path=f"{basefolder}/25-apr-{model}_{inout}_{run}_{fold}.pdf"
    )


def main():
    dec19()
    apr25()


if __name__ == '__main__':
    main()