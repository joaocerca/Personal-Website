from flask import render_template
import pandas as pd
import pymysql
import pymysql.cursors
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import os
from os import getenv, path, environ
from dotenv import load_dotenv

# load_dotenv("./.env")

ssh_host = environ.get("FLASK_MYSQL_DATABASE_HOST")
ssh_username = environ.get("FLASK_SSH_USERNAME")
ssh_password = environ.get("FLASK_SSH_PASSWORD")
database_username = environ.get("FLASK_MYSQL_DATABASE_USER")
database_password = environ.get("FLASK_MYSQL_DATABASE_PASSWORD")
database_name = environ.get("FLASK_MYSQL_DATABASE_DB1")
localhost = int(environ.get("LOCALHOST", "3306"))

def open_ssh_tunnel(verbose=False):
    """Open an SSH tunnel and connect using a username and password.
    
    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """
    
    # if verbose:
    #     sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    #
    # global tunnel
    # tunnel = SSHTunnelForwarder(
    #     (ssh_host, 22),
    #     ssh_username = ssh_username,
    #     ssh_password = ssh_password,
    #     remote_bind_address = ('127.0.0.1', 3306),
    #     allow_agent=False
    # )
    #
    # tunnel.start()

    pass
    

def mysql_connect():

    """
    Direct MySQL connection - simplified for Docker

    """
    # Global connection was giving too many 502 Gateway errors in the server
    connection = pymysql.connect(
        host=ssh_host,
        port=localhost,
        user=database_username,
        passwd=database_password,
        db=database_name,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection

def run_query(sql_query, commit, connection):

    with connection.cursor() as cursor:

        cursor.execute(sql_query)
        result = cursor.fetchall()

    if commit:
        connection.commit()

    return result


def run_query_to_df(sql, connection):
    return pd.read_sql_query(sql, connection)


def mysql_disconnect(connection):
    connection.close()



