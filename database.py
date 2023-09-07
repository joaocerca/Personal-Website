from flask import Blueprint, render_template, flash, redirect, url_for
from flask_login import UserMixin, login_required
from .addons.forms import ArtistForm, ReleaseForm, TrackForm
from .addons import dbconnection as dbconnect
from wtforms.validators import ValidationError
from pymysql.err import IntegrityError
import pandas as pd
# from addons import dbconnection as dbconnect
from os import getenv


database = Blueprint('database', __name__, template_folder="templates")

dbconnect.open_ssh_tunnel()
dbconnect.mysql_connect()

@database.route("/backend/database/tblartist")
@login_required
def db_artist_show():

    # dbconnect.mysql_connect()
    result = dbconnect.run_query("SELECT * FROM tbl_artist", 0)
    return render_template('artists_query.html', list=result)
    


@database.route("/backend/database/tblrelease")
@login_required
def db_release_show():
    # dbconnect.mysql_connect()
    result = dbconnect.run_query("SELECT * FROM tbl_release", 0)
    return render_template('releases_query.html', list=result)
   

@database.route("/backend/database/tbltrack")
@login_required
def db_track_show():

    # dbconnect.mysql_connect()
    result = dbconnect.run_query("SELECT * FROM tbl_track ORDER BY releaseid ASC", 0)
    return render_template('tracks_query.html', list=result)
    


@database.route('/backend/database/addArtist', methods=['GET','POST'])
@login_required
def add_artist():

    artistForm = ArtistForm()

    dbconnect.mysql_connect()

    if artistForm.validate_on_submit():
        artistName = artistForm.artistName.data
        artistCountry = artistForm.artistCountry.data

        sql_query = f'INSERT INTO tbl_artist (artistname, artistcountry) VALUES("{artistName}","{artistCountry}")'

        try:
            dbconnect.run_query(sql_query, 1)
            flash(f'{artistName} added to the DB')
        except IntegrityError:
            flash(f'{artistName} is already in the DB!')

    return render_template("add_artist.html", artistForm=artistForm)   



@database.route("/backend/database/addRelease", methods=['GET','POST'])
@login_required
def add_release():

    releaseForm = ReleaseForm()
        
    dbconnect.mysql_connect()
    
    if releaseForm.validate_on_submit():
            
        artistName = releaseForm.artistName.data
        releaseName = releaseForm.releaseName.data
        releaseFormat = releaseForm.releaseFormat.data
        releaseYear = releaseForm.releaseYear.data
        isCompilation = releaseForm.isCompilation.data
        releaseDesc = releaseForm.releaseDesc.data
        recordingType = releaseForm.recordingType.data
        releaseNotes = releaseForm.releaseNotes.data
        releaseNoTracks = releaseForm.releaseNoTracks.data
        releaseLength = releaseForm.releaseLength.data

        sql_query = f'INSERT INTO tbl_release VALUES ("{releaseName}","{releaseFormat}","{releaseYear}",(SELECT artistid FROM tbl_artist WHERE artistname = "{artistName}"),"{isCompilation}","{releaseDesc}","{recordingType}","{releaseNotes}",CONCAT(releaseformat, LEFT(artistid,4), LEFT(recordingtype,1), FLOOR(1 + (RAND() * 9999)), iscompilation),"{releaseNoTracks}",CONVERT("{releaseLength}", TIME));'
        dbconnect.run_query(sql_query, 1)
                    
        flash(f'{releaseName} added to the DB')

        return redirect(url_for('database.add_release'))
    
    return render_template("add_release.html", releaseForm=releaseForm)


@database.route("/backend/database/addTrack", methods=['GET', 'POST'])
@login_required
def add_track():

    trackForm = TrackForm()

    dbconnect.mysql_connect()    
     
    if trackForm.validate_on_submit():
            
        artistName = trackForm.artistName.data
        releaseName = trackForm.releaseName.data
        trackTitle = trackForm.trackTitle.data
        trackNo = trackForm.trackNo.data
        trackLength = trackForm.trackLength.data       


        print(artistName, releaseName, trackTitle, trackNo, trackLength)            
        
        # 'artists_releases_ids' is a View inside the DB
        sql_query = f'INSERT INTO tbl_track (trackTitle, trackno, tracklength, releaseid) SELECT "{trackTitle}","{trackNo}",CONVERT("{trackLength}", TIME), releaseid FROM artists_releases_ids WHERE releasename = "{releaseName}" AND artistname = "{artistName}";'
                    
        dbconnect.run_query(sql_query, 1)
    
        flash(f'{trackTitle} added to the DB')

        return redirect(url_for('database.add_track'))
    
    return render_template("add_track.html", trackForm=trackForm)


# dbconnect.mysql_disconnect()
# dbconnect.close_ssh_tunnel()