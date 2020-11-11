from skyfield.api import EarthSatellite
from skyfield.api import load
import math
from sgp4.api import Satrec , jday
import sqlite3
from datetime import datetime
connection = sqlite3.connect('satellite.db')
cursor = connection.cursor()

command_select = """SELECT id, name, tle FROM Satellite_TLE"""
command_create = """CREATE TABLE IF NOT EXISTS Satellite_data(id INTEGER PRIMARY KEY, tle_id INTEGER, satnum INTEGER, latitude REAL, longitude REAL,
elevation REAL, error_code INTEGER, rx REAL, ry REAL, rz REAL, vx REAL, vy REAL, vz REAL, velocity REAL, epoch_year REAL, epoch_days REAL,
bstar REAL, inclination REAL, right_ascension  REAL, eccentricity REAL, argument_of_perigee REAL, mean_anomaly REAL, mean_motion REAL, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (tle_id) REFERENCES Satellite_TLE (id));"""

command_insert = """INSERT OR IGNORE INTO Satellite_data(tle_id, satnum, latitude, longitude,elevation, error_code, rx, ry, rz, vx, vy, vz, velocity, epoch_year, epoch_days,bstar, inclination, right_ascension, eccentricity, argument_of_perigee, mean_anomaly, mean_motion) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""

cursor.execute(command_create)
cursor.execute(command_select)
records = cursor.fetchall()

while True:
    ts = load.timescale()
    t = ts.now()

    time = datetime.utcnow()
    jd, fr = jday(time.year, time.month, time.day, time.hour, time.minute, time.second)

    for row in records:
        id = row[0]
        name=row[1]
        line1, line2 = row[2].splitlines()
        satellite = EarthSatellite(line1, line2, name, ts)
        geocentric = satellite.at(t)
        subpoint = geocentric.subpoint()
        # Latitude and Longitude in degrees
        latitude = subpoint.latitude.degrees
        longitude = subpoint.longitude.degrees
        # Elevation in meters
        elevation = int(subpoint.elevation.m)
        sat=Satrec.twoline2rv(line1, line2)
        # e  Non zero error code if the satellite position could not be computed
        e, r, v = sat.sgp4(jd, fr)
        # rx, ry,rz   satellite position in kilometers
        rx,ry,rz = r
        # vx, vy,vz  satelite velocity in km/s
        vx,vy,vz = v
        # Velocity km/s
        velocity = math.sqrt(vx**2 + vy**2 +vz**2)
        # The unique satellite NORAD catalog number given in the TLE file
        satnum = sat.satnum
        # Full four-digit year of this element set’s epoch moment
        epochyr = sat.epochyr
        # Fractional days into the year of the epoch moment
        epochdays = sat.epochdays
        # Ballistic drag coefficient B* in inverse earth radii
        bstar = sat.bstar
        # Inclination in radians
        inclo = sat.inclo
        # Right ascension of ascending node in radians
        nodeo = sat.nodeo
        # Eccentricity
        ecco = sat.ecco
        # Argument of perigee in radians
        argpo = sat.argpo
        # Mean anomaly in radians
        mo = sat.mo
        # Mean motion in radians per minute
        no_kozai = sat.no_kozai
        cursor.execute(command_insert, (id, satnum, latitude, longitude,elevation, e, rx, ry, rz, vx, vy, vz, velocity, epochyr, epochdays, bstar, inclo, nodeo, ecco, argpo, mo, no_kozai))
    connection.commit()
cursor.close
connection.close()
