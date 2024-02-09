from . import db
from flask_login import UserMixin
from sqlalchemy.sql import func


class Triage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.String(150))
    content = db.Column(db.String(10000))
    date = db.Column(db.DateTime(timezone=True), default=func.now())
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))


class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(150), unique=True, index=True)
    password = db.Column(db.String(300))
    key = db.Column(db.String(300), index=True)
    triages = db.relationship('Triage')


class Flight(db.Model):
    id = db.Column(db.Integer, primary_key=True)

    departureStation = db.Column(db.String(3), nullable=False, index=True)
    arrivalStation = db.Column(db.String(3), nullable=False, index=True)
    departureCountryCode = db.Column(db.String(2), nullable=False, index=True)
    arrivalCountryCode = db.Column(db.String(2), nullable=False, index=True)

    departureDate = db.Column(db.DateTime, nullable=False, index=True)
    arrivalDate = db.Column(db.DateTime, nullable=False, index=True)

    departureDay = db.Column(db.DateTime, nullable=True, index=True)
    arrivalDay = db.Column(db.DateTime, nullable=True, index=True)

    price = db.Column(db.Numeric(precision=10, scale=2), nullable=False, index=True)
    currencyCode = db.Column(db.String(5), nullable=False, index=True)

    company = db.Column(db.String(30), nullable=False, index=True)

    scrapeDate = db.Column(db.DateTime, nullable=False, index=True)
    ticketUrl = db.Column(db.String(500))

    direction = db.Column(db.Integer, nullable=False, index=True)  # outbound == 0 vs return == 1
    type = db.Column(db.Integer, nullable=False, index=True)  # direct == 0 vs overlay == 1

    overlayCities = db.Column(db.Integer, db.ForeignKey('overlay.id'), nullable=True, index=True)

    flightNumber = db.Column(db.String(20), nullable=True, index=True)
    terminal = db.Column(db.String(20), nullable=True, index=True)
    availableSeats = db.Column(db.Integer, nullable=True, index=True)
    carrier = db.Column(db.String(20), nullable=True, index=True)

    hash = db.Column(db.String(30), index=True)


class Overlay(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    departureDate = db.Column(db.DateTime, nullable=True, index=True)
    arrivalDate = db.Column(db.DateTime, nullable=True, index=True)

    departureStation = db.Column(db.String(3), nullable=False, index=True)
    arrivalStation = db.Column(db.String(3), nullable=False, index=True)
    departureCountryCode = db.Column(db.String(2), nullable=False, index=True)
    arrivalCountryCode = db.Column(db.String(2), nullable=False, index=True)


class FlightTimesHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source = db.Column(db.String(20), nullable=True, index=True)  # flightradar for example

    departureStation = db.Column(db.String(3), index=True)
    arrivalStation = db.Column(db.String(3), index=True)

    expectedDepartureDate = db.Column(db.DateTime, nullable=False, index=True)
    realDepartureDate = db.Column(db.DateTime, nullable=False, index=True)

    expectedArrivalDate = db.Column(db.DateTime, nullable=False, index=True)
    realArrivalDate = db.Column(db.DateTime, nullable=False, index=True)

    flight = db.Column(db.Integer, db.ForeignKey('flight.id'))
    company = db.Column(db.String(20), nullable=True, index=True)

    flightNumber = db.Column(db.String(20), nullable=True, index=True)
    terminal = db.Column(db.String(20), nullable=True, index=True)
    gate = db.Column(db.String(20), nullable=True, index=True)
    availableSeats = db.Column(db.Integer(), nullable=True, index=True)
    carrier = db.Column(db.String(20), nullable=True, index=True)
