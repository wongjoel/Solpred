"""
Plot single day with metrics
"""
# Standard Library Modules
from pathlib import Path
from datetime import datetime
from datetime import timedelta
import math
import tempfile
import shutil

# External Modules
import pandas as pd
import matplotlib.pyplot as plt
import sklearn.metrics as metrics
from PIL import Image

#https://stackoverflow.com/questions/59587603/matplotlib-dashed-line-between-points-if-one-condition-is-met
#https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.vlines.html
#https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.between_time.html

def line_timeseries(day_df, current_time, img_base, actual, left_model, right_model, filebase="plot", output_dir=Path("")):
    fig, ax = plt.subplots(ncols=2)
    fig.set_size_inches(16, 9)

    # Image
    img_path = f"{img_base}/{current_time.to_pydatetime().strftime('%Y-%m-%d_%H-%M-%S')}.jpg"
    with Image.open(img_path) as img:
        ax[0].imshow(img)
    ax[0].axis("off")
    ax[0].set_title("Current Image")

    # Plot 
    context_offset = timedelta(minutes=4)
    window_size = timedelta(minutes=16)
    horizon_size = timedelta(minutes=7)

    start_time = current_time - window_size
    end_time = current_time + horizon_size
    context_start = start_time - context_offset
    context_end = end_time + context_offset

    df = day_df.between_time(context_start.to_pydatetime().time(), context_end.to_pydatetime().time(), include_start=True, include_end=True, axis=None)
    pred_df = day_df.between_time(current_time.to_pydatetime().time(), context_end.to_pydatetime().time(), include_start=True, include_end=True, axis=None)
    rmse_left = math.sqrt(metrics.mean_squared_error(df[actual], df[left_model]))
    rmse_right = math.sqrt(metrics.mean_squared_error(df[actual], df[right_model]))

    ymin = df[[actual, left_model, right_model]].min().min()
    ymax = df[[actual, left_model, right_model]].max().max()

    ax[1].vlines(start_time, ymin=ymin, ymax=ymax, linestyles="dotted")
    ax[1].vlines(current_time, ymin=ymin, ymax=ymax, linestyles="dotted")
    ax[1].vlines(end_time, ymin=ymin, ymax=ymax, linestyles="dotted")
    
    ax[1].plot(df.index, df[actual], label="actual")
    ax[1].plot(pred_df.index, pred_df[left_model], label="Fully Conv")
    ax[1].plot(pred_df.index, pred_df[right_model], label="SUNSET")
    
    plt.xlabel('Timestep')
    plt.ylabel('GHI (W/m2)')
    plt.legend(loc="upper left")
    ax[1].set_title(f"Fully Conv RMSE: {rmse_left:.1f} SUNSET RMSE: {rmse_right:.1f}")
    #fig.suptitle(fig_title)
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
    zip_file = f"/data/blackmountain/images/2015/{date}.zip"
    with tempfile.TemporaryDirectory() as td:
        shutil.unpack_archive(zip_file, td)
        print(f"Extracted to {td}")
        img_base = f"{td}/{date}"
        df1 = read_df_date(left_file, date)
        df2 = read_df_date(right_file, date)

        day_df = df1.join(df2, rsuffix="right", sort=True)

        counter = 0
        out_path = Path("./frames")
        out_path.mkdir()
        for index, row in day_df.iterrows():
            line_timeseries(day_df, index, img_base, actual, left_model, right_model, filebase=f"frame-{counter:04d}", output_dir=out_path)
            counter = counter + 1
    
def main():
    plot_day(left_file="/work/processed-runs/blackmountain-round/wide/16x60s_420s_fully-conv_run_00_fold_0_wide.csv.gz", 
            right_file="/work/processed-runs/blackmountain-round/wide/16x60s_420s_sunset_run_00_fold_0_wide.csv.gz",
            actual="actual",
            date="2015-06-19",
            left_model="fully-conv_16x60s_420s_run_00_fold_0",
            right_model="sunset_16x60s_420s_run_00_fold_0")


if __name__ == '__main__':
    main()