from flask import Blueprint, render_template, render_template_string, make_response
from ..addons import dbconnection as dbconnect
import pandas as pd
from bokeh.embed import file_html, components
from bokeh.io import show
from bokeh.models import ColumnDataSource, DataTable, TableColumn
from bokeh.resources import CDN
from bokeh.plotting import figure
from collections import OrderedDict, defaultdict
import json
import math


musicdashboard = Blueprint('musicdashboard', __name__, template_folder="templates")

dbconnect.open_ssh_tunnel()
dbconnect.mysql_connect()

@musicdashboard.route("/musicdashboard")
def show_dashboard():

    cdn_files = CDN.js_files

    script, div = stats_top_countries_plot()
    

    return render_template('music_dashboard.html', script=script, div=div, header=cdn_files)





def stats_top_countries_plot():

    top_countries_df = dbconnect.run_query_to_df("SELECT artistcountry AS 'Country', COUNT(*) AS 'Stats' FROM `tbl_artist` GROUP BY artistcountry ORDER BY Stats DESC LIMIT 10;")

    total_countries = dbconnect.run_query("SELECT COUNT(*) FROM tbl_artist", False)[0][0]

    top_countries_df.insert(2, "%", round(((top_countries_df['Stats']/total_countries)*100),1))

    source = ColumnDataSource(top_countries_df)
    columns = [TableColumn(field=col, title=col) for col in top_countries_df.columns]    
    data_table = DataTable(source=source, columns=columns, width=200, height=300, index_position=None, header_row=False)
 
    script, div = components(data_table)
    div_new = "<div class=\"col\" " + div[4:]
    print(len(components(data_table)))    

    return script, div_new



def stats_tracks_plot():

    return 1



def stats_format_plot():

    return 1


#     # result = dbconnect.run_query("SELECT releaseformat, COUNT(*) AS 'Soma', COUNT(*) AS 'Perc' FROM tbl_release GROUP BY releaseformat", 0)
#     # print(type(result))
#     # print(result)

#     releaseformat_df = dbconnect.run_query_to_df("SELECT releaseformat, COUNT(*) AS 'Soma', COUNT(*) AS 'Perc' FROM tbl_release GROUP BY releaseformat", 'releaseformat')

#     print(releaseformat_df)

#     df_totals = (releaseformat_df['Soma'].sum())

#     print(df_totals)

#     releasetotals = releaseformat_df.div([1,df_totals/100], axis='columns')

#     print(releasetotals)


# # # 

#     return render_template('dashboard.html', totals=])
#     # return render_template('dashboard.html', list=)


