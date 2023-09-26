from flask import Blueprint, render_template, render_template_string, make_response
from ..addons import dbconnection as dbconnect
import pandas as pd
import math
from math import pi
from bokeh.embed import file_html, components
from bokeh.io import show
from bokeh.models import (ColumnDataSource, DataTable, TableColumn, Plot, Range1d, AnnularWedge, Legend, LegendItem, GlyphRenderer, GeoJSONDataSource, LinearColorMapper, ColorBar)
from bokeh.resources import CDN
from bokeh.plotting import figure
from bokeh.palettes import all_palettes
from bokeh.transform import cumsum
from bokeh.io.doc import curdoc

from collections import OrderedDict, defaultdict
from werkzeug import exceptions
import numpy as np
import json

import geopandas as gpd


musicdashboard = Blueprint('musicdashboard', __name__, template_folder="templates")

dbconnect.open_ssh_tunnel()
dbconnect.mysql_connect()

@musicdashboard.route("/musicdashboard")
def show_dashboard():

    cdn_files = CDN.js_files

    results = stats_rel_trx_plot() 
    script1, div1 = stats_format_plot()
    script2, div2 = stats_countries_plot_map()
    script3, div3 = stats_releases_plot()

    stats_countries_plot_map()
    

        
    return render_template('music_dashboard.html', script_formats=script1, script_countries=script2, script_releases=script3, div_formats=div1, div_countries=div2, div_releases=div3, header=cdn_files, results=results)

def stats_rel_trx_plot():
    
    cnx = dbconnect.mysql_connect()

    rt_len_df = dbconnect.run_query_to_df("SELECT SEC_TO_TIME(AVG(TIME_TO_SEC(`Total Duration`))) AS `rel_avg_len`, SEC_TO_TIME(AVG(TIME_TO_SEC(`tracklength`))) AS `trck_avg_len` FROM durations_view; ", connection=cnx)
    longest_rel_df = dbconnect.run_query_to_df("SELECT DISTINCT `Artist`, `Release Name`, `Total Duration` FROM `durations_view` ORDER BY `Total Duration` DESC LIMIT 1;", connection=cnx).rename(columns={'Artist':'artist', 'Release Name':'release_name','Total Duration':'total_duration'})
    shortest_rel_df = dbconnect.run_query_to_df("SELECT DISTINCT `Artist`, `Release Name`, `Total Duration` FROM `durations_view` WHERE `Total Duration` IS NOT NULL ORDER BY `Total Duration` ASC LIMIT 1;",connection=cnx).rename(columns={'Artist':'artist', 'Release Name':'release_name','Total Duration':'total_duration'})
    longest_trx_df = dbconnect.run_query_to_df("SELECT * FROM durations_view WHERE tracklength IS NOT NULL ORDER BY `durations_view`.`tracklength` DESC LIMIT 1;",connection=cnx).rename(columns={'Artist':'artist', 'Release Name':'release_name','tracktitle':'track_title','tracklength':'tracklength'})
    shortest_trx_df = dbconnect.run_query_to_df("SELECT DISTINCT * FROM durations_view WHERE tracklength IS NOT NULL ORDER BY `durations_view`.`tracklength` ASC LIMIT 1;",connection=cnx).rename(columns={'Artist':'artist', 'Release Name':'release_name','tracktitle':'track_title','tracklength':'tracklength'})


    rt_len_df.rel_avg_len = rt_len_df.rel_avg_len.astype(str).map(lambda x: x[7:15])
    rt_len_df.trck_avg_len = rt_len_df.trck_avg_len.astype(str).map(lambda x: x[7:15])
    longest_rel_df.total_duration = longest_rel_df.total_duration.astype(str).map(lambda x: x[7:15])
    shortest_rel_df.total_duration = shortest_rel_df.total_duration.astype(str).map(lambda x: x[7:15])
    longest_trx_df.tracklength = longest_trx_df.tracklength.astype(str).map(lambda x: x[7:15])
    shortest_trx_df.tracklength = shortest_trx_df.tracklength.astype(str).map(lambda x: x[7:15])
    
    dbconnect.mysql_disconnect(cnx)
    

    results = {"rel_avg_len":rt_len_df.rel_avg_len[0],
                "trx_avg_len":rt_len_df.trck_avg_len[0],
                "long_release_name":longest_rel_df.release_name[0],
                "long_release_artist":longest_rel_df.artist[0],
                "long_release_duration":longest_rel_df.total_duration[0],
                "short_release_name":shortest_rel_df.release_name[0],
                "short_release_artist":shortest_rel_df.artist[0],
                "short_release_duration":shortest_rel_df.total_duration[0],
                "long_track_name":longest_trx_df.track_title[0],
                "long_track_release":longest_trx_df.release_name[0],
                "long_track_artist":longest_trx_df.artist[0],
                "long_track_duration":longest_trx_df.tracklength[0],
                "short_track_name":shortest_trx_df.track_title[0],
                "short_track_release":shortest_trx_df.release_name[0],
                "short_track_artist":shortest_trx_df.artist[0],
                "short_track_duration":shortest_trx_df.tracklength[0]
                }
    
    return results


def stats_format_plot():

    cnx = dbconnect.mysql_connect()

    query = "SELECT tbl_format.formatdesc AS formatdesc, COUNT(tbl_release.releasename) AS total_numbers FROM `tbl_release` JOIN `tbl_format` ON tbl_release.releaseformat = tbl_format.formatname GROUP BY releaseformat;"

    format_numbers_df = dbconnect.run_query_to_df(query, connection=cnx).set_index('formatdesc')

    dbconnect.mysql_disconnect(cnx)


    color = {
        "Compact Disc": "lightskyblue",
        "Compact Disc + Disc Versatile Disc": "sandybrown",
        "Digital Versatile Disc": "orangered",
        "Digital": "crimson",
        "Extended Play": "gold",
        "Cassette":"mediumorchid",
        "Super audio CD": "mediumvioletred",
        "Vinyl": "forestgreen"
    }

    # Gets the available formats inside the df from the SQL Query
    formats = format_numbers_df.index.to_list()

    # Gets the colors of the available formats from the SQL Query
    avail_colors = [color[format] for format in formats]

    # Gets the values for each format
    data_raw = format_numbers_df["total_numbers"].to_dict()

    # zip() aggregates both lists
    format_colors_dict = dict(zip(formats, avail_colors))

    # Calculates the share of each element of the DataFrame of the SQL query result
    share = format_numbers_df.total_numbers.map(lambda x: x/format_numbers_df.total_numbers.sum()*100)

    # Calculates the angles for each % of element
    angles = share.map(lambda x: 2*pi*(x/100)).cumsum().to_list() 

    format_numbers_source = ColumnDataSource(dict(        
        start  = [0] + angles[:-1],
        end    = angles,
        format = formats,
        colors = [color[format] for format in formats],
        values = [data_raw[format] for format in formats],
        share = share
    ))
 
    p = figure(width=650, height=450, toolbar_location="right", tools="hover", tooltips="@format: @values (@share %)")

    p.annular_wedge(x=0, y=0, inner_radius=0.35, outer_radius=0.6,
                     start_angle='start', end_angle='end',
                     line_color="white", line_width=0.5, fill_color="colors", legend_field="format", source=format_numbers_source)
    
    
    p.axis.axis_label = None
    p.axis.visible = False
    p.grid.grid_line_color = None

    p.title.text ="Total number of releases by format"
    p.title.align = "center"
    p.title.text_font_size = "25px"

    script, div = components(p)
    div_new = "<div class=\"col\" " + div[4:]

    return script, div_new


def stats_releases_plot():
   
    cnx = dbconnect.mysql_connect()

    releases_numbers_df = dbconnect.run_query_to_df("SELECT releaseyear, COUNT(*) AS 'releases_per_year' FROM `tbl_release` GROUP BY releaseyear ASC;", connection=cnx)

    dbconnect.mysql_disconnect(cnx)

    years = releases_numbers_df["releaseyear"].tolist()
    releases = releases_numbers_df["releases_per_year"].tolist()
    
    tooltips = [("Year","@x"), ("Number of releases", "@top")]

    p = figure(sizing_mode="stretch_width", tools="pan,wheel_zoom,box_zoom,reset,hover", tooltips=[*tooltips])
    p.vbar(x=years, top=releases, width=0.7, bottom=0, line_join='bevel')
    p.y_range.start = 0  


    # Title components
    p.title.text = "Number of releases by year"
    p.title.align = "center"
    p.title.text_font_size = "25px"

    script, div = components(p)

    return script, div


def stats_countries_plot_map():

    pd.set_option('display.max_columns', None)

    cnx = dbconnect.mysql_connect()
   
    top_countries_df = dbconnect.run_query_to_df("SELECT artistcountry AS 'Country', COUNT(*) AS 'no_of_artists' FROM `tbl_artist` WHERE artistcountry NOT LIKE '%/%' GROUP BY artistcountry ORDER BY COUNT(*) DESC;", connection=cnx)
    total_countries = dbconnect.run_query("SELECT COUNT(*) FROM tbl_artist", False, connection=cnx)[0][0]

    top_countries_df.to_csv("dbresults.csv")

    dbconnect.mysql_disconnect(cnx)

    # print(top_countries_df)

    shapefile = 'website/static/maps/ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp'

    gdf = gpd.read_file(shapefile)[['ADMIN', 'ISO_A2_EH', 'geometry']]


    gdf.rename(columns={'ADMIN':'country','ISO_A2_EH':'country_code','geometry':'geometry'}, inplace=True)

    # gdf['country'] == 'Antartica'] returns the df and checks which line is 'Antartica'
    # gdf[gdf['country'] == 'Antarctica'] returns the position of 'Antartica' 

    gdf = gdf.drop(gdf.index[159])


    merged = gdf.merge(top_countries_df,left_on='country_code', right_on='Country', how='outer')
    merged['Country'] = merged['Country'].combine_first(merged['country_code'])
    
    # in order to generate the map, the data needs to be put into json and then GeoJSONDataSource
    merged_json = json.loads(merged.to_json())
    json_data = json.dumps(merged_json)    
    geosource = GeoJSONDataSource(geojson=json_data)


    palette = all_palettes['Iridescent'][14]    
    color_mapper = LinearColorMapper(palette=palette, low = 0.0, high = merged.no_of_artists.max())

    # Creates the color bar. 
    color_bar = ColorBar(color_mapper=color_mapper, label_standoff=8,width = 500, height = 20, border_line_color=None,location = (0,0), orientation = 'horizontal')

    # Creates the figure object.
    p = figure(height=650, width=1200, toolbar_location = 'right', tooltips="@country: @no_of_artists")
    p.xgrid.grid_line_color = 'grey'
    p.ygrid.grid_line_color = 'grey'
    p.title.text = "Number of artists by country"
    p.title.align = "center"
    p.title.text_font_size = "25px"

    #Add patch renderer to figure. 
    p.patches('xs','ys', source=geosource, color={'field' :'no_of_artists', 'transform':color_mapper},line_color = 'black', line_width = 0.25, fill_alpha = 1)
    p.add_layout(color_bar, 'below')

    script, div = components(p)
    div_new = "<div class=\"col\" " + div[4:]

    return script, div_new


### Table with top countries ###
# def stats_top_countries_plot():

#     cnx = dbconnect.mysql_connect()
   
#     top_countries_df = dbconnect.run_query_to_df("SELECT artistcountry AS 'Country', COUNT(*) AS 'No of Artists' FROM `tbl_artist` GROUP BY artistcountry ORDER BY COUNT(*) DESC LIMIT 15;", connection=cnx)
#     total_countries = dbconnect.run_query("SELECT COUNT(*) FROM tbl_artist", False, connection=cnx)[0][0]
   
#     top_countries_df.insert(2, "% of artists per country", round(((top_countries_df['No of Artists']/total_countries)*100),1))

#     source = ColumnDataSource(top_countries_df)
#     columns = [TableColumn(field=col, title=col) for col in top_countries_df.columns]    
#     data_table = DataTable(source=source, columns=columns,  height=475, index_position=None, header_row=True, row_height=30)
    
#     script, div = components(data_table)
#     div_new = "<div class=\"col\" " + div[4:]

#     dbconnect.mysql_disconnect(cnx)
    

#     return script, div_new


