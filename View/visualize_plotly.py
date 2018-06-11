import plotly.graph_objs as go , os
from plotly.graph_objs import *
from plotly.offline import plot as offpy

def scatter3d(traces, columns, title, out_path):
    data = []
    for trace in traces:
        data.append(go.Scatter3d(
                        x=trace[columns[0]].values,
                        y=trace[columns[1]].values,
                        z=trace[columns[2]].values,
                        mode='markers',

                        marker=dict(
                            size=6,
                            #line=dict(
                                #color='rgba(217, 217, 217, 0.14)',
                                #width=5
                            #),
                            opacity=0.8
                        )
                    ))
    layout = go.Layout(
        title=title,
        #hovermode='closest',
        scene = Scene(
            xaxis = XAxis(title=columns[0]),
            yaxis = YAxis(title=columns[1]),
            zaxis = ZAxis(title=columns[2])
        ),
        showlegend=True
    )
    fig = go.Figure(data=data, layout=layout)

    out_file = os.path.join(out_path, str(title) + ".html")
    offpy(fig, filename=out_file, auto_open=True, show_link=False)
    #with open("scatter3.txt", 'w') as fw:
    #    fw.write(offpy(fig, show_link=False, include_plotlyjs=False, output_type='div'))
    return offpy(fig, show_link=False, include_plotlyjs=False, output_type='div')


def plotonmap(df,data,plotname= 'plot on map',color='blue'):
    import os
    from bokeh.embed import file_html
    from bokeh.resources import CDN
    from bokeh.io import show, output_file
    from bokeh.models import (GMapPlot, GMapOptions, ColumnDataSource, Circle, DataRange1d, PanTool, WheelZoomTool, BoxSelectTool)
    os.environ.get("foo")

    lat = 35.6892
    lon = 51.3890
    zoom = 7
    api_key = 'AIzaSyCvllwPPFlcF6SF8e2WJBhErb9IYOuqUyk'
    map_type = 'roadmap'

    if "lat" in data : lat = data['lat']
    if "lon" in data : lon = data['lon']
    if "zoom" in data: zoom = data['zoom']
    if "api_key" in data: api_key = data['api_key']
    if "map_type" in data: map_type = data['map_type']

    map_options = GMapOptions(lat=lat, lng=lon, map_type=map_type, zoom=zoom)

    api_key = os.environ.get('API_KEY')
    api_key = 'AIzaSyCvllwPPFlcF6SF8e2WJBhErb9IYOuqUyk'

    plot = GMapPlot(x_range=DataRange1d(), y_range=DataRange1d(), map_options=map_options, api_key=api_key)
    plot.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool())

    baseline = df['count merchant'].min()
    m = df['count merchant'].mean() / zoom
    source = ColumnDataSource(data=dict(lat=df['lat'].values.tolist(),
                                        lon=df['lon'].values.tolist(),
                                        rad=[((i - baseline) / m) + zoom for i in
                                             df['count merchant'].values.tolist()]))

    circle = Circle(x="lon", y="lat", size="rad", fill_color=color, fill_alpha=0.3)

    plot.add_glyph(source, circle)
    return file_html(plot, CDN, "my plot",plotname)