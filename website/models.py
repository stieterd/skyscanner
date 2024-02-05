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
    email = db.Column(db.String(150), unique=True)
    password = db.Column(db.String(300))
    key = db.Column(db.String(300))
    triages = db.relationship('Triage')

class Flight(db.Model):
    id = db.Column(db.Integer, primary_key=True)

    departureStation = db.Column(db.String(3), nullable=False)
    arrivalStation = db.Column(db.String(3), nullable=False)
    departureCountryCode = db.Column(db.String(2), nullable=False)
    arrivalCountryCode = db.Column(db.String(2), nullable=False)

    departureDate = db.Column(db.DateTime, nullable=False)
    arrivalDate = db.Column(db.DateTime, nullable=False)

    departureDay = db.Column(db.DateTime, nullable=True)
    arrivalDay = db.Column(db.DateTime, nullable=True)

    price = db.Column(db.Numeric(precision=10, scale=2), nullable=False)
    currencyCode = db.Column(db.String(5), nullable=False)

    company = db.Column(db.String(30), nullable=False)

    scrapeDate = db.Column(db.DateTime, nullable=False)
    ticketUrl = db.Column(db.String(500))
    hash = db.Column(db.String(30))
