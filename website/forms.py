from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, DateField, DecimalField, IntegerField, DateTimeLocalField
from wtforms.validators import Length, NumberRange, Optional


class RequestForm(FlaskForm):
    class OptionalIfEmpty(Length):
        def __call__(self, form, field):
            if field.data and len(field.data.strip()) == 0:
                field.errors[:] = []
                return
            super(RequestForm.OptionalIfEmpty, self).__call__(form, field)

    departure_city = SelectField('Departure city',
                                 choices=[('AMS', 'AMS'), ('RTM', 'RTM'), ('EIN', 'EIN'), ('MST', 'MST'), ('NRN', 'NRN'),
                                          ('BRU', 'BRU')])
    arrival_city = StringField('Arrival city (Optional)',
                               validators=[OptionalIfEmpty(max=3, message="Use city code of 3 characters")])
    departure_date_first = DateField('Your first feasible departure date', format='%Y-%m-%d')
    departure_date_last = DateField('Your last feasible departure date', format='%Y-%m-%d')
    arrival_date_first = DateField('Your first feasible arrival date', format='%Y-%m-%d')
    arrival_date_last = DateField('Your last feasible arrival date', format='%Y-%m-%d')

    min_days_stay = IntegerField('Minimum days stay (Optional)',
                                 validators=[Optional(), NumberRange(min=1, message="Minimum 1 day")])
    max_days_stay = IntegerField('Maximum days stay (Optional)',
                                 validators=[Optional(), NumberRange(min=1, message="Minimum 1 day")])

    airport_radius = IntegerField('Airport radius (km) (Optional)',
                                  validators=[Optional(), NumberRange(min=0, message="Minimum 0 km")])

    max_price_per_flight = IntegerField("Max price flight (euro) (Optional)", validators=[Optional(), NumberRange(min=20, message="Minimum 20 euro")])
