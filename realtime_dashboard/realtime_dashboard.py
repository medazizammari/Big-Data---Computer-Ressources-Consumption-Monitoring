import random
import pandas as pd
from bokeh.driving import count
from bokeh.models import ColumnDataSource
from kafka import KafkaConsumer
from bokeh.plotting import curdoc, figure
from bokeh.models import DatetimeTickFormatter
from bokeh.models.widgets import Div
from bokeh.layouts import column, row
import ast
import time
import pytz
from datetime import datetime
import colorcet as cc
from functools import partial
from threading import Thread, Lock
import json


def updatecomputerData(event):

    event = ast.literal_eval(event.value.decode("utf-8"))

    plot = get_computer_plot(int(event["computer_id"]))
    x = datetime.fromtimestamp(event["timestamp"], tz).isoformat()
    x = pd.to_datetime(x)
    plot["div"].text = "<h2>Computer {id}</h2> <br> battery :{battery}%".format(
        id=event["computer_id"], battery=event["battery"])

    plot["sources"]["network_usage_src"].stream(
        {"x": [x], "y": [event["network_usage"]]}, ROLLOVER)
    plot["sources"]["cpu_usage_src"].stream(
        {"x": [x], "y": [event["cpu_usage"]]}, ROLLOVER)
    plot["sources"]["memory_usage_src"].stream(
        {"x": [x], "y": [event["memory_usage"]]}, ROLLOVER)


def updatecomputersStats(event):
    event = ast.literal_eval(event.value.decode("utf-8"))
    for computer_index in event["computer_id"]:
        computer_id=event["computer_id"][computer_index]
        plot = get_computer_plot(int(computer_id))
        
        text="<h2>Stats</h2>"
        for stat_label in event:
            text+="{stat_label} : {value} <br>".format(stat_label=stat_label,value=int(event[stat_label][computer_index]))
        plot["stats_div"].text = text;
        
            
    


def consumecomputerData():
    for event in computerDataConsumer:
        doc.add_next_tick_callback(partial(updatecomputerData, event=event))


def consumecomputersStats():
    for event in computersStatsConsumer:
        doc.add_next_tick_callback(partial(updatecomputersStats, event=event))


def get_computer_plot(computer_id):
    with critical_function_lock:
        computer_plot = None
        for plot in plots:
            if plot["computer_id"] == computer_id:
                computer_plot = plot
                break
        if(computer_plot == None):

            div = Div(
                text="<h2>Computer {id}</h2>".format(id=str(computer_id)),
                width=90,
                height=35
            )

            stats_div = Div(
                text="",
                width=250,
                height=35
            )

            network_usage_fig = figure(title="Disk Usage",
                                    x_axis_type="datetime")

            network_usage_src = ColumnDataSource({"x": [], "y": []})
            network_usage_fig.line("x", "y", source=network_usage_src,
                                color=cc.glasbey_dark[0])

            cpu_usage_fig = figure(title="Memory Usage",
                                                x_axis_type="datetime")

            cpu_usage_src = ColumnDataSource({"x": [], "y": []})
            cpu_usage_fig.line("x", "y", source=cpu_usage_src,
                                            color=cc.glasbey_dark[1])

            memory_usage_fig = figure(title="CPU usage",
                                                x_axis_type="datetime")

            memory_usage_src = ColumnDataSource({"x": [], "y": []})
            memory_usage_fig.line("x", "y", source=memory_usage_src,
                                            color=cc.glasbey_dark[1])

            computer_plot = {"computer_id": computer_id,
                            "sources": {"network_usage_src": network_usage_src,
                                        "cpu_usage_src": cpu_usage_src,
                                        "memory_usage_src": memory_usage_src,
                                        },
                            "div": div,
                            "stats_div": stats_div
                            }
            plots.append(computer_plot)

            doc.add_root(
                row(children=[
                    div,
                    stats_div,
                    network_usage_fig,
                    cpu_usage_fig,
                    memory_usage_fig]
                    )
            )
        return computer_plot


ROLLOVER = 10

plots = []

tz = pytz.timezone('Africa/Tunis')

source = ColumnDataSource({"x": [], "y": []})

computerDataConsumer = KafkaConsumer('JsoncomputerData', auto_offset_reset='latest', bootstrap_servers=[
    'kafka:9092'], consumer_timeout_ms=20000)

computersStatsConsumer = KafkaConsumer('JsoncomputersStats', auto_offset_reset='latest', bootstrap_servers=[
    'kafka:9092'], consumer_timeout_ms=20000)

doc = curdoc()

critical_function_lock = Lock()

thread = Thread(target=consumecomputerData)
thread.start()


