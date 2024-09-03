# Import the dependencies.
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import os
import datetime 
from datetime import datetime, timedelta
from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
#engine = create_engine("sqlite:///Resources/hawaii.sqlite")
base_dir = os.path.abspath(os.path.dirname(__file__))
database_path = os.path.join(base_dir, "Resources", "hawaii.sqlite")
engine = create_engine(f"sqlite:///{database_path}")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)


# Save references to each table

Measurement = Base.classes.measurement
Station= Base.classes.station 


# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)


#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br>"
        f"/api/v1.0/stations<br>"
        f"/api/v1.0/tobs<br>"
        f"/api/v1.0/<start><br>"
        f"/api/v1.0/<start>/<end>"
    )
    
@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the last 12 months of precipitation data."""
    # Get the last date in the dataset
    last_date_str = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
    one_year_ago = (last_date - timedelta(days=365)).strftime('%Y-%m-%d')
    
    
    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago).all()
    
    # Check if results are being fetched
    print(f"Number of Records Retrieved: {len(results)}")
    
    # Convert the query results to a dictionary
    precip_data = {}
    for date, prcp in results:
        if date in precip_data:
            precip_data[date] += prcp if prcp else 0
        else:
            precip_data[date] = prcp if prcp else 0
    
    return jsonify(precip_data)

@app.route("/api/v1.0/stations")
def stations():
    """Return a list of all stations."""
    results = session.query(Station.station).all()
    stations = [station[0] for station in results]
    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def tobs():
    """Return temperature observations (tobs) for the most active station for the last year."""
    # Get the last date in the dataset
    last_date_str = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
    one_year_ago = (last_date - timedelta(days=365)).strftime('%Y-%m-%d')
    
    # Find the most active station
    most_active_station = session.query(
        Measurement.station
    ).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()[0]

    print(f"Using Most Active Station: {most_active_station}")
    print(f"Date One Year Ago: {one_year_ago}")
    
    # Perform a query to retrieve the temperature observations for the most active station
    results = session.query(Measurement.date, Measurement.tobs)\
        .filter(Measurement.station == most_active_station)\
        .filter(Measurement.date >= one_year_ago)\
        .all()
    
    # Check if results are being fetched
    print(f"Number of Temperature Observations Retrieved: {len(results)}")
    
    # Convert the query results to a list of dictionaries
    tobs_data = [{"date": date, "tobs": tobs} for date, tobs in results]
    
    return jsonify(tobs_data)


@app.route("/api/v1.0/<start>")
def start_date(start):
    """Return min, max, and avg temperatures from the start date to the end of the dataset."""
    
    # Query to calculate the minimum, maximum, and average temperatures from the start date
    results = session.query(
        func.min(Measurement.tobs),
        func.avg(Measurement.tobs),
        func.max(Measurement.tobs)
    ).filter(Measurement.date >= start).all()
    
    # Structure the results in a dictionary
    temps = {
        "Start Date": start,
        "Min Temp": results[0][0],
        "Avg Temp": results[0][1],
        "Max Temp": results[0][2]
    }
    
    return jsonify(temps)

@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start, end):
    """Return min, max, and avg temperatures between the start and end dates."""
    
    # Query to calculate the minimum, maximum, and average temperatures between the start and end dates
    results = session.query(
        func.min(Measurement.tobs),
        func.avg(Measurement.tobs),
        func.max(Measurement.tobs)
    ).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    
    # Structure the results in a dictionary
    temps = {
        "Start Date": start,
        "End Date": end,
        "Min Temp": results[0][0],
        "Avg Temp": results[0][1],
        "Max Temp": results[0][2]
    }
    
    return jsonify(temps)


# Run the app
if __name__ == '__main__':
    app.run(debug=True)