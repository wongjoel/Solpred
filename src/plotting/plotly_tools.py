import os
import re
from pathlib import Path
from pprint import pformat
from typing import Union

from plotly.graph_objs import Figure
from plotly.graph_objs.scattergl import Legendgrouptitle
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import sklearn
import numpy as np
import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import seaborn as sns
from matplotlib.colors import ListedColormap
import webbrowser

#Logging
import logging
import logging.config
import json

with open("logging_config.json") as f:
    log_config = json.load(f)
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


def boxplot_by_category(df, title, group_col=None, out_dir='.', saveHTML=True, show_plot=False, height=800, xtitle=None, ytitle=None, boxpoints='suspectedoutliers', fence_percentiles=None, verbose=0):
    '''
    Draws boxplots grouped on the x axis by unique values of ``group_col``.
    :param df: Dataframe to plot
    :param title: plot title
    :param group_col: the name of a column to group the data by.  A box is drawn for each group of data (in all other columns) for each unique value in this column.
    :param out_dir:
    :param saveHTML:
    :param show_plot:
    :param height:
    :param xtitle:
    :param ytitle:
    :param boxpoints: how can be 'all','outliers', 'suspectedoutliers' or False.  See https://plotly.com/python/box-plots/
    :param fence_percentiles: custom percentiles for the upper and lower fence (whisker-end) positions. Eg, for the 5th and 95th percentiles, use fence_percentile=[5,95].  None uses plotly defaults.
    :return:
    '''
    PLOTLY_TEMPLATE = "seaborn"
    TEMPLATE_COLOURS = pio.templates[PLOTLY_TEMPLATE].layout["colorway"]

    # Get list of indices and models to iterate through
    models = pd.unique(df.columns)

    # Get unique colour for each model
    colours = {m: TEMPLATE_COLOURS[ii % len(TEMPLATE_COLOURS)] for ii, m in enumerate(models)}

    # Create the figure
    fig = go.Figure()
    for c in sorted(set(df.columns) - {group_col}):
        data = df[[c, group_col]].dropna()
        data = data.sort_values(group_col)
        fig = go.Figure()
        if fence_percentiles is None:
            fig.add_trace(
                go.Box(
                    x=list(map(str, data[group_col])),
                    y=data[c],
                    name=c,
                    legendgroup=c,
                    text="{}:".format(c),
                    line_color=colours[c],
                    boxpoints=boxpoints,
                    boxmean=True
                )
            )

        else:
            fig.add_trace(
                go.Box(
                    x=pd.unique(list(map(str, data[group_col]))),
                    name=c,
                    legendgroup=c,
                    text="{}:".format(c),
                    line_color=colours[c],
                    boxpoints=boxpoints,
                    boxmean=True,
                    q1=data.groupby(group_col)[c].agg(np.percentile, ([25])).values,
                    median=data.groupby(group_col)[c].agg(np.percentile, ([50])).values,
                    mean=data.groupby(group_col)[c].agg(np.mean).values,
                    sd=data.groupby(group_col)[c].agg(np.std).values,
                    q3=data.groupby(group_col)[c].agg(np.percentile, ([75])).values,
                    lowerfence=data.groupby(group_col)[c].agg(np.percentile, ([fence_percentiles[0]])).values,
                    upperfence=data.groupby(group_col)[c].agg(np.percentile, ([fence_percentiles[1]])).values,
                    orientation='v',
                )
            )
            fig.update_layout(
                boxmode='group'
            )

    if xtitle is not None: fig.update_xaxes(title=xtitle)
    if ytitle is not None: fig.update_yaxes(title=ytitle)
    fig.update_layout(title=title, boxmode="group")
    fig.update_layout(height=height)

    path = out_dir + '/' + (title + " (Box).html")
    if show_plot:
        fig.show()
    if saveHTML:
        plotly.offline.plot(fig, filename=f'{out_dir}/{title}.html', include_plotlyjs='cdn', auto_open=False)
        if verbose > 0:
            logger.info(f'Plot saved to {os.path.abspath(out_dir + "/" + title)}.html')
        # fig.write_html(path)
    return fig


def boxplot_by_column(df, title, out_dir='.', saveHTML=True, show_plot=False, height=800, xtitle=None, ytitle=None, boxpoints='suspectedoutliers', fence_percentiles=None, verbose=0):
    '''
    Draw boxplots, showing one box for each dataframe column.
    :param df: Dataframe to plot
    :param title: plot title
    :param out_dir:tim
    :param saveHTML:
    :param show_plot:
    :param height:
    :param xtitle:
    :param ytitle:
    :param boxpoints: how can be 'all','outliers', 'suspectedoutliers' or False.  See https://plotly.com/python/box-plots/
    :param fence_percentiles: custom percentiles for the upper and lower fence (whisker-end) positions. Eg, for the 5th and 95th percentiles, use fence_percentile=[5,95].  None uses plotly defaults.
    :return:
    '''
    PLOTLY_TEMPLATE = "seaborn"
    TEMPLATE_COLOURS = pio.templates[PLOTLY_TEMPLATE].layout["colorway"]

    # Get list of indices and models to iterate through
    models = pd.unique(df.columns)

    # Get unique colour for each model
    colours = {m: TEMPLATE_COLOURS[ii % len(TEMPLATE_COLOURS)] for ii, m in enumerate(models)}

    # Create the figure
    fig = go.Figure()
    for c in df.columns:
        if fence_percentiles is None:
            fig.add_trace(
                go.Box(
                    y=df[c],
                    name=c,
                    legendgroup=c,
                    text="{}: ".format(c),
                    line_color=colours[c],
                    boxpoints=boxpoints,
                    boxmean=True,
                )
            )
        else:
            fig.add_trace(
                go.Box(
                    y=df[c],
                    name=c,
                    legendgroup=c,
                    text="{}: ".format(c),
                    line_color=colours[c],
                    boxpoints=boxpoints,
                    boxmean=True,
                    q1=[np.percentile(df[c], 25)],
                    median=[np.percentile(df[c], 50)],
                    mean=[np.mean(df[c])],
                    sd=[np.std(df[c])],
                    q3=[np.percentile(df[c], 75)],
                    lowerfence=[np.percentile(df[c], fence_percentiles[0])],
                    upperfence=[np.percentile(df[c], fence_percentiles[1])],
                    orientation='v',
                )
            )
            fig.update_layout(
                boxmode='group'
            )

    if xtitle is not None: fig.update_xaxes(title=xtitle)
    if ytitle is not None: fig.update_yaxes(title=ytitle)
    fig.update_layout(title=title, boxmode="group")
    fig.update_layout(height=height)

    path = out_dir + '/' + (title + " (Box).html")
    if show_plot:
        fig.show()
    if saveHTML:
        plotly.offline.plot(fig, filename=f'{out_dir}/{title}.html', include_plotlyjs='cdn', auto_open=False)
        if verbose > 0:
            logger.info(f'Plot saved to {os.path.abspath(out_dir + "/" + title)}.html')
        # fig.write_html(path)
    return fig


def bar(df, title, out_dir='.', error_df=None, saveHTML=True, show_plot=False, show_menu=True, height=800):
    '''
    Draws each columns of a dataframe as a separate trace in a plotly plot.
    :param df:
    :param title:
    :param out_dir:
    :param error_df:
    :param saveHTML:
    :param show_plot:
    :param draw_mode:
    :return:
    '''
    import plotly.graph_objects as go
    fig = go.Figure()
    cols = df.columns
    for col in cols:
        td = df[[col]]
        fig.add_trace(go.Bar(x=td.index, y=td[col], name=col, hoverlabel=dict(namelength=-1)))
    fig.update_layout(barmode='group', hovermode='x', title=title)
    fig.update_layout(height=height)
    fig.update_layout(yaxis=dict(autorange=True, fixedrange=False))

    if (error_df is not None):
        fig.update_layout(annotations=[
            go.layout.Annotation(
                showarrow=False,
                text=error_df.to_string().replace('\n', '<br>'),
                xanchor='left',
                x=df.index[0],
                xshift=0,
                yanchor='top',
                y=0.05,
                font=dict(
                    family="Courier New, monospace",
                    size=16
                )
            )])
    fig['layout']['xaxis'].update(side='bottom')

    if show_menu:
        updatemenus = list([
            dict(active=1,
                 buttons=list([
                     dict(label='Log Scale',
                          method='update',
                          args=[{'visible': [True, True]},
                                {'yaxis': {'type': 'log'}}]),
                     dict(label='Linear Scale',
                          method='update',
                          args=[{'visible': [True, False]},
                                {'yaxis': {'type': 'linear'}}])
                 ]),
                 )
        ])

        layout = dict(updatemenus=updatemenus)
        fig.update_layout(layout)

    if show_plot:
        fig.show()
    if saveHTML:
        fig_to_html(fig, out_dir, title)
    return fig


def scatter(df: pd.DataFrame, title='', out_dir='.', trend_degree: int = [1], colour_col: str = None, saveHTML=True, show_plot=False, draw_mode='markers', height=800):
    """
    A 2D scatter plot using the first two columns of `df`.
    :param df:
    :param title:
    :param out_dir:
    :param trend_degree: list of degrees of polynomial fits to apply to the scatter, eg. 1==linear, 1==quadratic etc.
    :param colour_col:
    :param saveHTML:
    :param show_plot:
    :param draw_mode:
    :param height:
    :return:
    """
    fig = go.Figure()

    if df.shape[1] != 2:
        logger.warn(f'Choosing first two columns for scatter from df with {len(df.columns)} columns')
    # TODO build a scatterplot matrix if n_cols>2

    ''' Loop through dataframes in df_in making a subplot from each '''
    cols = df.columns
    fig.add_trace(go.Scattergl(x=df[cols[0]], y=df[cols[1]], mode=draw_mode, name=f'{cols[0]} vs {cols[1]}', hoverlabel=dict(namelength=-1), fill=colour_col, marker=dict(opacity=0.5)))

    fig.update_layout(barmode='group', hovermode='x', title=title)
    fig.update_layout(height=height)

    fig.update_layout(yaxis=dict(autorange=True, fixedrange=False))
    fig.update_xaxes(title=cols[0])
    fig.update_yaxes(title=cols[1])
    fig['layout']['xaxis'].update(side='bottom')
    fig['data'][0]['showlegend'] = True

    if trend_degree is not None:
        for deg in trend_degree:
            from sklearn.linear_model import LinearRegression
            from sklearn.preprocessing import PolynomialFeatures
            from sklearn.pipeline import make_pipeline
            from sklearn.metrics import r2_score, mean_absolute_error
            regr = make_pipeline(PolynomialFeatures(deg), LinearRegression())

            r = df.dropna().sort_values(cols[0])
            x = r[cols[0]]
            y = r[cols[1]]
            model = regr.fit(x.values.reshape(-1, 1), y.values)
            fit = model.predict(x.values.reshape(-1, 1))
            fig.add_trace(go.Scatter(x=x.values, y=fit, mode="lines", name=f"Poly({deg}) fit (R2={r2_score(y, fit):.2f}, MAE={mean_absolute_error(y, fit):.2f})"))

    if show_plot:
        fig.show()
    if saveHTML:
        fig_to_html(fig, out_dir, title)
    return fig


def heatmap(df: pd.DataFrame, title: str, out_dir: str, colour_scale: str = 'Turbo', log_scale: bool = True, height: int = 2000, raster_width: int = 2000, save_html: bool = True, raster_format: str = 'webp'):
    """
    Generates a heatmap using the DataFrame row index and column names as the axis and the values as the colours.

    :param df: The dataframe to plot
    :param title: title / filename
    :param out_dir: dir to save in (for save_html=True and raster_format not None)
    :param colour_scale: plotly colour scale name to use
    :param log_scale: if True, will internally generate a logarithmic colour scale and apply it.
    :param height: plot height (for html and raster outputs)
    :param raster_width: image width (for raster output only)
    :param save_html: saves an html file if True
    :param raster_format: saves a raster file if set to a valid file type supported by plotly/kaleido, eg: png, webp, bmp, jpg etc.
    """
    scale = colour_scale

    if log_scale:
        scale = px.colors.sequential.Turbo
        scale = [
            [0, scale[0]],
            [1. / 10000, scale[1]],
            [1. / 5000, scale[2]],
            [1. / 1000, scale[4]],
            [1. / 500, scale[5]],
            [1. / 100, scale[7]],
            [1. / 50, scale[9]],
            [1. / 10, scale[11]],
            [1. / 5, scale[13]],
            [1., scale[14]],
        ]

    fig = px.imshow(df, aspect="auto", color_continuous_scale=scale)
    fig.update_layout(title=title, height=2000)

    if save_html:
        fig_to_html(fig, str(out_dir), title)

    if raster_format is not None:
        fig.write_image(f"{out_dir}/{title}.{raster_format}", width=2000, height=2000)

def time_series(df_in: Union[pd.DataFrame, tuple], title='', out_dir='.', table_df=None, df_precision=3, saveHTML=True, show_plot=False, draw_mode='lines', subplot_titles: list[str]=None, height=800, verbose=0, show_menu=False, intervals: list[str]=None) -> Figure:
    '''
    Draws each columns of a dataframe as a separate trace in a plotly plot.
    You can also pass in a tuple of dataframes, and have each rendered as a subplot

    :param df_in: a single dataframe, or a tuple of dataframes with a common index - each will be plotted on a separate sub-plot eg.
    >>> (df[['col1', 'col2']], df[['col3']]

    :param df_in: dataframe to plot, or tuple of dataframes to generate subplots from
    :param title: title string, also forms filename if saved
    :param out_dir: dir to save plot to, if enabled
    :param table_df: optional dataframe to render as an html table at the top of the plot
    :param df_precision: precision at which to render the table_df
    :param saveHTML: whether to save the plot as html
    :param show_plot: whether to show the plot after saving.
    :param draw_mode: how to draw the plot traces - eg 'lines', 'markers', 'lines+markers'
    :param subplot_titles: optional titles for subplots, must be the same length as df_in which must also be a tuple
    :param height: height of plot (or subplots) in pixels
    :param intervals: an optional list of two column-name suffix regex strings that, if matched with a column name suffix, designate matching columns to be upper/lower bounds for rendering as fill around a normal trace line.
           Eg: ['_upper', '_lower'] will pair up columns 'a' with columns 'a_upper' and 'a_lower' for drawing a fill.  Useful for shading a single confidence intervals etc.

           TODO: This can also directly specify a base_col:[[upper1,lower1],[upper2,lower2]] column mapping directly, which is used for specifying multiple confidence interval columns (upper1, lower1 etc)  around a single column/line.
           Eg: 'value_col': [('value_col_p90_low', 'value_col_p90_high'), ('value_col_p50_low', 'value_col_p50_high')] will draw two shaded intervals around value_col.

    :param verbose: >1 to show log output
    :return: the plotly Figure

    '''
    import plotly.graph_objects as go
    subplot_height = height
    if isinstance(df_in, tuple) and len(df_in) > 1:
        fig = make_subplots(rows=len(df_in), cols=1, shared_xaxes=True, subplot_titles=subplot_titles, vertical_spacing=0.01)
        height = height * len(df_in)
    else:
        df_in = tuple([df_in])
        fig = go.Figure()

    ''' Loop through dataframes in df_in making a subplot from each '''
    for idx, df in enumerate(df_in):

        # Build a dict that maps normal columns to their upper/lower columns, if they exist.
        fill_cols = {}
        if intervals is not None:
            lower_regex = intervals[0]
            upper_regex = intervals[1]
            for c in df.columns:
                upper = [l for l in df.columns if re.match(c + upper_regex, l)]
                lower = [l for l in df.columns if re.match(c + lower_regex, l)]
                if len(upper)>0 and len(upper) == len(lower):
                    fill_cols[c] = list(zip(lower, upper))
                elif not re.match('.+' + upper_regex, c) and not re.match('.+' + lower_regex, c):
                    # if the col didn't have mupper/lower matches and it's not a match itself, it's a standalone column, so we just want to plot it.
                    fill_cols[c] = []
            if len(fill_cols) > 0:
                cols = list(fill_cols.keys())
            # Plotly likes the filled columns to be plotted first, so sort the dict accordingly
            fill_cols = {k: v for k, v in sorted(fill_cols.items(), key=lambda item: item[1], reverse=True)}
            cols = list(fill_cols.keys())
            if verbose > 0:
                logger.debug(f'Found upper/lower column matches: {pformat(fill_cols)}')
        else:
            cols = df.columns

        subplot_col = 1
        subplot_row = 1

        # Plot each column as a new
        for col in cols:
            td = df[[col]]
            if len(df_in) > 1:
                subplot_row = idx + 1
            lg_title = Legendgrouptitle({'text': subplot_titles[idx]}) if subplot_titles is not None else None
            if fill_cols.get(col) != [] and fill_cols.get(col) is not None:
                for upper, lower in fill_cols.get(col):
                    fig.add_trace(go.Scattergl(x=df.index, y=df[lower], fill='tonexty', mode=draw_mode, name=lower, hoverlabel=dict(namelength=-1), line=dict(width=0)), **{'row': subplot_row, 'col': subplot_col} if len(df_in) > 1 else {})
                    fig.add_trace(go.Scattergl(x=df.index, y=df[upper], fill='tonexty', mode=draw_mode, name=upper, hoverlabel=dict(namelength=-1), line=dict(width=0)), **{'row': subplot_row, 'col': subplot_col} if len(df_in) > 1 else {})
            fig.add_trace(go.Scattergl(x=td.index, y=td[col], mode=draw_mode, name=col, hoverlabel=dict(namelength=-1)), **{'row': subplot_row, 'col': subplot_col} if len(df_in) > 1 else {})

            # Make the filled intervals the same colour as their associated line.
            if fill_cols.get(col) != [] and fill_cols.get(col) is not None:
                for upper, lower in fill_cols.get(col):
                    c = get_trace_colour(fig, col)
                    set_trace_colour(fig, upper, hex_to_rgba(c, 0.05))
                    set_trace_colour(fig, lower, hex_to_rgba(c, 0.05))

        fig.update_layout(hovermode='closest', title=title)
        fig.update_layout(height=height)
        if isinstance(df_in, tuple) and len(df_in) > 1:
            fig.update_layout(legend_tracegroupgap=100)

        fig['layout']['xaxis'].update(side='bottom')

    # Add extra mode-bar buttons back
    fig.update_layout(modebar_add=["v1hovermode", "toggleSpikelines", 'drawline', 'hoverClosestGl2d', 'drawopenpath', 'drawclosedpath', 'drawcircle', 'drawrect', 'eraseshape'])

    if show_menu:
        y_scale_dropdown = list([
            dict(active=1,
                 buttons=list([
                     dict(label='Log Scale', method='update', args=[{'visible': [True, True]}, {'yaxis': {'type': 'log'}}]),
                     dict(label='Linear Scale', method='update', args=[{'visible': [True, False]}, {'yaxis': {'type': 'linear'}}])
                 ])
            )
        ])

        layout = dict(updatemenus=y_scale_dropdown)
        fig.update_layout(layout)

    if saveHTML:
        df_html = []
        if table_df is not None and len(table_df) > 0:
            df_html = df_to_html(table_df, precision=df_precision)
        html_file = figs_to_html([fig], f'{out_dir}/{title}', extra_html=df_html, verbose=verbose)

    if show_plot and not saveHTML:
        fig.show()
    elif show_plot and saveHTML:
        url = "file://" + os.path.abspath(html_file)
        webbrowser.open(url)

    return fig

def hex_to_rgba(hex_color: str, opacity: int=1.0) -> tuple:
    """
    Converts a hex colour string to an rgba object, optionally setting the alpha channel.

    See https://community.plotly.com/t/scatter-plot-fill-with-color-how-to-set-opacity-of-fill/29591
    """
    hex_color = hex_color.lstrip("#")
    if len(hex_color) == 3:
        hex_color = hex_color * 2
    rgb = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    fillcolor = f"rgba{(*rgb, int(opacity*255))}"
    return fillcolor




def scatterplot_3d(df, colour_col: str = None, title='', out_dir='.', saveHTML=True, show_plot=False, height=1200):
    """
    Draws a 3D scatterplot using the DF's first 3 columns.
    :param df:
    :param colour_col:
    :param title:
    :param out_dir:
    :param saveHTML:
    :param show_plot:
    :param height:
    :return:
    """

    import plotly.express as px
    fig = px.scatter_3d(df, x=df.columns[0], y=df.columns[1], z=df.columns[2], color=colour_col)

    fig.update_layout(height=height)
    if show_plot:
        fig.show()
    if saveHTML:
        fig_to_html(fig, out_dir, title)


def histogram_overlaid(df: pd.DataFrame, nbins: int = 200, title='', out_dir='.', saveHTML=True, show_plot=False, height=1200):
    """ Draws an overlaid histogram from each columns in the DF """
    m = df.melt().dropna()
    fig = px.histogram(m, x="value", color="variable", nbins=nbins, marginal="box", opacity=0.7)
    fig.update_layout(barmode='overlay')
    fig.update_traces(opacity=0.75)
    fig.update_xaxes(title=title)

    fig.update_layout(height=height)
    if show_plot:
        fig.show()
    if saveHTML:
        fig_to_html(fig, out_dir, title)


def scatterplot_matrix(df, colour_col: str = None, title='', out_dir='.', saveHTML=True, show_plot=False, height=800):
    """
    Draws a NxN scatterplot matrix, where N=len(df.columns).  See https://plotly.com/python/splom/
    :param df:
    :param colour_col:
    :param title:
    :param out_dir:
    :param saveHTML:
    :param show_plot:
    :param height:
    :return:
    """
    fig = px.scatter_matrix(df, color=colour_col, opacity=0.5)

    fig.update_layout(height=height)

    if show_plot:
        fig.show()
    if saveHTML:
        fig_to_html(fig, out_dir, title)


def set_trace_colour(fig, trace_name: str, colour: str):
    for i, data in enumerate(fig['data']):
        if data['name'] == trace_name:
            data['line']['color'] = colour

def get_trace_colour(fig, trace_name: str):
    '''
    Attempts to return the colour of a specific trace
    :param fig: the ploty figure
    :param trace_name: the name of the trace
    :return: the trace colour, or None if not found.
    '''

    colors = px.colors.qualitative.Plotly

    colour = None
    for i, data in enumerate(fig['data']):
        name = data['name']

        if name == trace_name:
            colour = data['line']['color']  # if the colour was supplied to add_trace explicitly

        if colour is None:
            colour = colors[i]  # if the automatic colour palette was used isntead
    return colour


def fig_to_html(fig, out_dir, title, verbose=0):
    plotly.offline.plot(fig, filename=f'{out_dir}/{title}.html', include_plotlyjs='cdn', auto_open=False)
    if verbose > 0:
        logger.info(f'Plot saved to {os.path.abspath(out_dir + "/" + title)}.html')


def figs_to_html(figs: list, file: str, show=False, extra_html=[], verbose=0):
    '''
    Saves multiple independent Plotly figures to a single HTML page, optionally prepended with an arbitrary html block.
    :param figs: list fo figures to save
    :param file: .html file path to save to
    :param show: whether to show after saving.
    :param: extra_html: an arbitrary html block to place above the plot - eg a data table, heading, description etc.
    :param verbose: whether to log the saved file success/location etc.
    :return:
    '''

    html_file = f'{file}.html'
    parent_dir = Path(html_file).parent

    if not parent_dir.exists():
        parent_dir.mkdir(exist_ok=True, parents=True)

    with open(html_file, 'w') as f:
        for html in extra_html:
            f.write(html)
        for p in figs:
            f.write(p.to_html(full_html=False, include_plotlyjs='cdn'))
        if verbose > 0:
            logger.info(f'Plot saved to {os.path.abspath(html_file)}')
    if show:
        url = "file://" + os.path.abspath(html_file)
        webbrowser.open(url)
    return html_file


def df_to_html(df: pd.DataFrame, file_name: str = None, show: bool = False, shade_cols: list = None, bar_cols: list = None, cmap: str = 'muted', min_style='color:blue', max_style='color:red', precision=2):
    '''
    Converts a dataframe to an HTML file using Pandas styling options, and optionally saves and/or opens it in a web browser.
    :param df: dataframe to convert
    :param file_name: file_name to save the html to, or None to disable.
    :param show: whether to show the HTML file in a browser (only if file_name is also not None).  None to disable.
    :param cmap: a seaborn.color_palette() argument string to use for the colour-map for cell background highlighting, or None to disable.  See https://seaborn.pydata.org/tutorial/color_palettes.html and https://medium.com/@morganjonesartist/color-guide-to-seaborn-palettes-da849406d44f
    :return: the HTML string
    '''
    if not df.index.is_unique or not df.columns.is_unique:
        logger.error(f'Returning blank html for DF because column and index values must be unique for HTML rendering, but df.index.is_unique={df.index.is_unique}, df.columns.is_unique={df.columns.is_unique}. Try dropping duplicates first.')
        return ""

    df = df.copy().round(precision)
    with pd.option_context("display.precision", precision):
        # cm = sns.light_palette("red", as_cmap=True)
        cm = ListedColormap(sns.color_palette(cmap))

        styler = df.style

        styler.set_properties(**{'font-size': '9pt', 'font-family': 'monospace'})
        # styler.set_properties(**{"selector": "", "props": [("border", "1px solid lightgrey")]})

        if shade_cols is not None:
            num_cols = set(df.select_dtypes(include=['int16', 'int32', 'int64', 'float16', 'float32', 'float64']).columns)  # only shade numeric cols
            shade_cols = num_cols.intersection(shade_cols) - set(bar_cols if bar_cols is not None else [])  # don't shade bar cols
            styler.background_gradient(cmap=cm, subset=list(shade_cols))

        if bar_cols is not None:
            pal = sns.color_palette(cmap, n_colors=len(bar_cols)).as_hex()
            num_cols = set(df.select_dtypes(include=['int16', 'int32', 'int64', 'float16', 'float32', 'float64']).columns)  # only shade numeric cols
            bar_cols = num_cols.intersection(bar_cols) - set(shade_cols if shade_cols is not None else [])  # don't shade bar cols
            for idx, b in enumerate(bar_cols):
                styler.bar(subset=[b], color=pal[idx])

        if min_style is not None:
            styler.apply(highlight_min, attr=min_style, axis=0)

        if max_style is not None:
            styler.apply(highlight_max, attr=max_style, axis=0)

        html = styler.highlight_null('#FFC7CE').to_html()

        if file_name is not None:
            with open(file_name, "w") as text_file:
                print(html, file=text_file)

        if file_name is not None and show:
            import webbrowser
            url = "file://" + os.path.abspath(file_name)
            webbrowser.open(url)

        ''' Add some extra styling'''
        html = html + '''<style  type="text/css" >
                            table, th {
                              text-align: right;
                              font-size: 10pt;
                              font-family: monospace;
                              border: 1px solid darkgrey;
                              border-collapse: collapse;
                            }
                            td {
                              text-align: right;
                              font-size: 10pt;
                              padding: 3px;
                              border: 1px solid lightgrey;                              
                            }
                        </style>  '''

    return html


def highlight_max(data: pd.DataFrame, attr='text-decoration: underline'):
    '''
    Adds Pandas styling attribute to the table and saves to HTML
    Example Usage:
    `
    html = (df.style
        .apply(highlight_max,  attr='text-decoration: underline', axis=0)
        .apply(highlight_max,  attr='font-weight:bold', axis=0)
        .render()
        )
    `
    @param data:
    @param attr: examples:  'text-decoration: underline', 'font-weight:bold', 'color:green', 'background-color:red', 'font-style: italic'
    @return: the dataframe with extra styling added.
    '''
    if data.ndim == 1:  # Series from .apply(axis=0) or axis=1
        is_max = data == data.max()
        if data.dtype in num_types:
            return [attr if v else '' for v in is_max]
        else:
            return ['' for v in is_max]
    else:  # from .apply(axis=None)
        num_cols = data.select_dtypes(include=num_types).columns
        is_max = data[num_cols] == data[num_cols].max().max()
        return pd.DataFrame(np.where(is_max, attr, ''), index=data.index, columns=data.columns)


num_types = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']


def highlight_min(data: pd.DataFrame, attr='text-decoration: underline'):
    '''
    Adds Pandas styling attribute to the table and saves to HTML
    Example Usage:
    `
    html = (df.style
        .apply(highlight_min,  attr='text-decoration: underline', axis=0)
        .apply(highlight_min,  attr='font-weight:bold', axis=0)
        .render()
        )
    `
    @param data:
    @param attr: examples:  'text-decoration: underline', 'font-weight:bold', 'color:green', 'background-color:red', 'font-style: italic'
    @return: the dataframe with extra styling added.
    '''
    if data.ndim == 1:  # Series from .apply(axis=0) or axis=1
        is_min = data == data.min()
        if data.dtype in num_types:
            return [attr if v else '' for v in is_min]
        else:
            return ['' for v in is_min]
    else:  # from .apply(axis=None)
        num_cols = data.select_dtypes(include=num_types).columns
        is_min = data[num_cols] == data[num_cols].min().min()
        return pd.DataFrame(np.where(is_min, attr, ''), index=data.index, columns=data.columns)


def plot_lat_long(df: pd.DataFrame, lat_col: str, long_col: str, out_dir: str, color_col: str = None, hover_cols: list = None, title: str = '', show_plot=False):
    '''
    Plots lat/longs from a dataframe on a map and renders to HTML with plotly.
    :param df: dataframe with lat/longs to plot
    :param lat_col: name of column with latitudes
    :param long_col: name of column with longitudes
    :param color_col: name of numerical column to use for coloruing rendered points
    :param hover_cols: columns to add to hover-tooltip for points
    :param title: plot title / filename
    :param out_dir:dir to save into.  None to not save.
    :param show_plot: True to open browser showing plot, False not to.
    :return: the plotly plot object.
    '''

    import plotly.express as px

    if lat_col in df.columns and long_col in df.columns:
        df = df.dropna(subset=[lat_col, long_col])

        # Work out the best zoom level to show all points
        zoom, center = zoom_center(
            lons=df[long_col].values,
            lats=df[lat_col].values
        )

        if len(df) > 0:
            fig = px.scatter_mapbox(df, lat=lat_col, lon=long_col, color=color_col, hover_data=hover_cols, zoom=zoom, center=center)
            fig.update_layout(mapbox_style="open-street-map")

            if show_plot:
                fig.show()
            if out_dir is not None:
                df_html = []
                figs_to_html([fig], f'{out_dir}/{title}', extra_html=df_html)
            return fig

        else:
            logger.warning(f'Cant plot lat/long points, no data remaining after dropping NAs from columns: {[lat_col, long_col, color_col]}')
    else:
        logger.warning(f'Cant plot lat/long points, one or more columns missing from: {[lat_col, long_col]}')


def zoom_center(lons: tuple = None, lats: tuple = None, lonlats: tuple = None, format: str = 'lonlat', projection: str = 'mercator', width_to_height: float = 2.0) -> (float, dict):
    """
    Finds optimal zoom and centering for a plotly mapbox.
    Must be passed (lons & lats) or lonlats.
    Temporary solution awaiting official implementation, see:
    https://github.com/plotly/plotly.js/issues/3434
    and
    https://stackoverflow.com/a/64148305/4771628

    Parameters
    --------
    lons: tuple, optional, longitude component of each location
    lats: tuple, optional, latitude component of each location
    lonlats: tuple, optional, gps locations
    format: str, specifying the order of longitud and latitude dimensions,
        expected values: 'lonlat' or 'latlon', only used if passed lonlats
    projection: str, only accepting 'mercator' at the moment,
        raises `NotImplementedError` if other is passed
    width_to_height: float, expected ratio of final graph's with to height,
        used to select the constrained axis.

    Returns
    --------
    zoom: float, from 1 to 20
    center: dict, gps position with 'lon' and 'lat' keys

    >>> print(zoom_center((-109.031387, -103.385460),
    ...     (25.587101, 31.784620)))
    (5.75, {'lon': -106.208423, 'lat': 28.685861})
    """
    if lons is None and lats is None:
        if isinstance(lonlats, tuple):
            lons, lats = zip(*lonlats)
        else:
            raise ValueError(
                'Must pass lons & lats or lonlats'
            )

    maxlon, minlon = max(lons), min(lons)
    maxlat, minlat = max(lats), min(lats)
    center = {
        'lon': round((maxlon + minlon) / 2, 6),
        'lat': round((maxlat + minlat) / 2, 6)
    }

    # longitudinal range by zoom level (20 to 1)
    # in degrees, if centered at equator
    lon_zoom_range = np.array([
        0.0007, 0.0014, 0.003, 0.006, 0.012, 0.024, 0.048, 0.096,
        0.192, 0.3712, 0.768, 1.536, 3.072, 6.144, 11.8784, 23.7568,
        47.5136, 98.304, 190.0544, 360.0
    ])

    if projection == 'mercator':
        margin = 1.2
        height = (maxlat - minlat) * margin * width_to_height
        width = (maxlon - minlon) * margin
        lon_zoom = np.interp(width, lon_zoom_range, range(20, 0, -1))
        lat_zoom = np.interp(height, lon_zoom_range, range(20, 0, -1))
        zoom = round(min(lon_zoom, lat_zoom), 2)
    else:
        raise NotImplementedError(
            f'{projection} projection is not implemented'
        )

    return zoom, center
