import numpy as np
import pandas as pd

import datetime
from dateutil.relativedelta import relativedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Station = Base.classes.station
Measurement = Base.classes.measurement

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################




@app.route("/")
def home():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all measurements
    results = session.query(Measurement.date).all()

    session.close()

    all_dates = list(np.ravel(results))

    # List all routes that are available.
    home_array = ["/api/v1.0/precipitation","/api/v1.0/stations","/api/v1.0/tobs","/api/v1.0/<start>","/api/v1.0/<start>/<end>",all_dates]
    return jsonify(home_array)




@app.route("/api/v1.0/precipitation")
def precipitation():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all measurements
    results = session.query(Measurement.date, Measurement.prcp).all()

    session.close()

    #Convert the query results to a dictionary using `date` as the key and `prcp` as the value.
    #Return the JSON representation of your dictionary.
    precipitation_dict = {}
    for date, prcp in results:
        precipitation_dict[date] = prcp


    return jsonify(precipitation_dict)




@app.route("/api/v1.0/stations")
def stations():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all measurements
    results = session.query(Station.station).all()

    session.close()

    #Return a JSON list of stations from the dataset.

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)




@app.route("/api/v1.0/tobs")
def tobs():

    session = Session(engine)

    # What are the most active stations? (i.e. what stations have the most rows)?

    station = []
    station_activity = []
    data = engine.execute("SELECT * FROM station")
    for record in data:
        station.append(record.station)
        station_count = session.query(Measurement).filter_by(station=record.station).count()
        station_activity.append(station_count)

    session.close()

    # List the stations and the counts in descending order.

    station_count = pd.DataFrame({'station' : station,'activity count' : station_activity})
    station_count = station_count.sort_values(by = ["activity count"],ascending=False)
    station_id = station_count.iloc[0].values[0]

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all measurements
    measurements = session.query(Measurement)

    measurement_dates = []
    for measurement in measurements:
        measurement_dates.append(measurement.date)  

    date = measurement_dates[-1]

    d = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    year_before = (d - relativedelta(years=1)).strftime('%Y-%m-%d')

    tobs = session.query(Measurement.tobs).filter(Measurement.date >= year_before).filter(Measurement.station == station_id).all()

    session.close()

    all_tobs = list(np.ravel(tobs))

    return jsonify(all_tobs)



@app.route("/api/v1.0/<start>")
def calc_temps_1(start):

    session = Session(engine)

    min_avg_max_tobs = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
       filter(Measurement.date >= start).all()

    session.close()

    all_tobs = list(np.ravel(min_avg_max_tobs))

    return jsonify(all_tobs)




@app.route("/api/v1.0/<start>/<end>")
def calc_temps_2(start, end):

    session = Session(engine)

    min_avg_max_tobs = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    
    session.close()

    all_tobs = list(np.ravel(min_avg_max_tobs))

    return jsonify(all_tobs)

if __name__ == '__main__':
    app.run(debug=True)