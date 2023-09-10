from flask import Blueprint, render_template, render_template_string, make_response
from ..addons import dbconnection as dbconnect
import pandas as pd
from bokeh.embed import file_html, components
from bokeh.io import show
from bokeh.models import ColumnDataSource, DataTable, TableColumn
from bokeh.resources import CDN
from bokeh.plotting import figure
from collections import OrderedDict, defaultdict
from werkzeug import exceptions
import numpy as np
import json
import math


musicdashboard = Blueprint('musicdashboard', __name__, template_folder="templates")

dbconnect.open_ssh_tunnel()
dbconnect.mysql_connect()

@musicdashboard.route("/musicdashboard")
def show_dashboard():

    cdn_files = CDN.js_files

    script, div = stats_top_countries_plot()
    stats_tracks_plot()

    return render_template('music_dashboard.html', script=script, div=div, header=cdn_files)



def stats_top_countries_plot():


    try:
        top_countries_df = dbconnect.run_query_to_df("SELECT artistcountry AS 'Country', COUNT(*) AS 'Stats' FROM `tbl_artist` GROUP BY artistcountry ORDER BY Stats DESC LIMIT 10;")
        total_countries = dbconnect.run_query("SELECT COUNT(*) FROM tbl_artist", False)[0][0]
    except exceptions.BadGateway:
        dbconnect.mysql_connect()
        top_countries_df = dbconnect.run_query_to_df("SELECT artistcountry AS 'Country', COUNT(*) AS 'Stats' FROM `tbl_artist` GROUP BY artistcountry ORDER BY Stats DESC LIMIT 10;")
        total_countries = dbconnect.run_query("SELECT COUNT(*) FROM tbl_artist", False)[0][0]


    top_countries_df.insert(2, "%", round(((top_countries_df['Stats']/total_countries)*100),1))

    source = ColumnDataSource(top_countries_df)
    columns = [TableColumn(field=col, title=col) for col in top_countries_df.columns]    
    data_table = DataTable(source=source, columns=columns, width=200, height=300, index_position=None, header_row=False)
 
    script, div = components(data_table)
    div_new = "<div class=\"col\" " + div[4:]
    

    return script, div_new



def stats_tracks_plot():

    results = {}


    try:
        rt_len_df = dbconnect.run_query_to_df("SELECT SEC_TO_TIME(AVG(TIME_TO_SEC(`Total Duration`))) AS `rel_avg_len`, SEC_TO_TIME(AVG(TIME_TO_SEC(`tracklength`))) AS `trck_avg_len` FROM durations_view; ")
        longest_rel_df = dbconnect.run_query_to_df("SELECT DISTINCT `Artist`, `Release Name`, `Total Duration` FROM `durations_view` ORDER BY `Total Duration` DESC LIMIT 1;")
        shortest_rel_df = dbconnect.run_query_to_df("SELECT DISTINCT `Artist`, `Release Name`, `Total Duration` FROM `durations_view` WHERE `Total Duration` IS NOT NULL ORDER BY `Total Duration` ASC LIMIT 1;")
    except exceptions.BadGateway:
        dbconnect.mysql_connect()
        rt_len_df = dbconnect.run_query_to_df("SELECT SEC_TO_TIME(AVG(TIME_TO_SEC(`Total Duration`))) AS `rel_avg_len`, SEC_TO_TIME(AVG(TIME_TO_SEC(`tracklength`))) AS `trck_avg_len` FROM durations_view; ")
        longest_rel_df = dbconnect.run_query_to_df("SELECT DISTINCT `Artist`, `Release Name`, `Total Duration` FROM `durations_view` ORDER BY `Total Duration` DESC LIMIT 1;")
        shortest_rel_df = dbconnect.run_query_to_df("SELECT DISTINCT `Artist`, `Release Name`, `Total Duration` FROM `durations_view` WHERE `Total Duration` IS NOT NULL ORDER BY `Total Duration` ASC LIMIT 1;")


    rt_len_df['rel_avg_len'] = rt_len_df['rel_avg_len'].astype(str).map(lambda x: x[7:15])
    rt_len_df['trck_avg_len'] = rt_len_df['trck_avg_len'].astype(str).map(lambda x: x[7:15])
    longest_rel_df['Total Duration'] = longest_rel_df['Total Duration'].astype(str).map(lambda x: x[7:15])
    shortest_rel_df['Total Duration'] = shortest_rel_df['Total Duration'].astype(str).map(lambda x: x[7:15])


    print(rt_len_df)
    print(longest_rel_df)
    print(shortest_rel_df)
    

    return 1



def stats_format_plot():

    return 1





