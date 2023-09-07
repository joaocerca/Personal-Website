from flask import Blueprint, render_template
from . import dbase

main = Blueprint('main', __name__, template_folder="templates")

@main.route('/')
def index():
    return render_template("index.html")

