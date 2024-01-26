from flask import Blueprint, render_template, request, flash, jsonify, redirect, url_for
from flask_login import login_required, current_user
from .models import Triage
from . import db
import json
import datetime
from .forms import RequestForm

from Request import Request
from Flight import Flight
import atexit
import threading
from apscheduler.schedulers.background import BackgroundScheduler

import os
import pandas
import time

OUTPUT_DIR = "output_data"

views = Blueprint('views', __name__)
flight = Flight.empty_flight()

def threaded_func():
    global flight
    while True:
        outputs = os.listdir(OUTPUT_DIR)
        outbound_files = [file for file in outputs if file.startswith('outbound')]
        return_files = [file for file in outputs if file.startswith('return')]
        # Find the latest outbound file
        latest_outbound = max(outbound_files,
                              key=lambda x: datetime.datetime.strptime(x.split('_')[1], '%Y-%m-%d-%H.csv'))
        # Find the latest return file
        latest_return = max(return_files,
                            key=lambda x: datetime.datetime.strptime(x.split('_')[1], '%Y-%m-%d-%H.csv'))
        flight = Flight(pandas.read_csv(f"{OUTPUT_DIR}/{latest_outbound}"),
                        pandas.read_csv(f"{OUTPUT_DIR}/{latest_return}"))
        print(len(flight.outbound_flights))
        print(len(flight.return_flights))
        print(time.time())
        print()
        time.sleep(10*60)


scheduler = BackgroundScheduler()
scheduler.add_job(func=threaded_func, trigger="interval", seconds=60)
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

def date_hook(json_dict):
    for (key, value) in json_dict.items():
        try:
            json_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except:
            pass
    return json_dict


@views.route('/home', methods=['GET', 'POST'])
@views.route('/', methods=['GET', 'POST'])
@login_required
def home():
    return render_template("home.html", user=current_user)


@views.route('/addTriage', methods=['GET'])
@login_required
def add_triage():
    if request.method == 'GET':
        new_triage = Triage(data="RouteSearch", user_id=current_user.id)
        db.session.add(new_triage)
        db.session.commit()

        return redirect(url_for("views.home"))


@views.route('/showResults/<int:triage_id>', methods=['GET'])
@login_required
def show_results(triage_id):
    if request.method == 'GET':
        p = Triage.query.filter_by(id=triage_id, user_id=current_user.id).first()
        content_dict = json.loads(p.content, object_hook=date_hook)

        ## TODO: THIS IS A RETARDED FIX, NEED TO CHANGE THIS ASAP
        del content_dict['csrf_token']
        del content_dict['view']
        for key, value in content_dict.items():
            if value == '':
                content_dict[key] = None

            try:
                content_dict[key] = float(value)
            except Exception as e:
                pass

        flight_request = Request(**content_dict)

        if flight.outbound_flights.empty and flight.return_flights.empty:
            return render_template('flight_results.html', flights=[],
                                   user=current_user)

        filtered_flight = flight.filter_flights(flight_request)

        result_df = filtered_flight.get_possible_return_flights_df(flight_request)
        # result_group = result_df.groupby('hash_x')
        # result_dict = result_group.apply(lambda x: x.to_dict(orient='records')).to_dict()

        # result_json = json.dumps(result_dict, default=Flight.date_json_encoder)
        # return jsonify({'outbound': json.loads(outbound_flights_json), 'return': json.loads(return_flights_json)})
        return render_template('flight_results.html', flights=result_df.to_dict(orient='records'), user=current_user)


@views.route('/triage/<int:triage_id>', methods=['GET', 'POST'])
@login_required
def triage(triage_id):
    p = Triage.query.filter_by(id=triage_id, user_id=current_user.id).first()
    form = RequestForm()
    # select_options = {
    #     'departure_city': ['Selecteer', 'AMS', 'RTM', 'EIN', 'NRN', 'BRU'],
    #     'arrival_city': ['Selecteer', 'BCN', 'BER', 'OTHER'],
    #     'airport_radius': ['Selecteer', '0', '25', '50', '100', '150']
    # }

    if request.method == 'POST' and form.validate_on_submit():
        # content = dict(request.form)
        if 'submit' in request.form:
            p.content = json.dumps(request.form)
            db.session.commit()
            return redirect(url_for('views.home'))

        elif 'view' in request.form:
            p.content = json.dumps(request.form)
            db.session.commit()
            return redirect(url_for('views.show_results', triage_id=triage_id))

    elif p is not None and p.content is not None:
        content_dict = json.loads(p.content, object_hook=date_hook)
        form.process(data=content_dict)

        return render_template('triage.html', user=current_user, p=p, form=form)
    else:
        return render_template('triage.html', user=current_user, p=p, form=form)


@views.route('/deleteTriage/<int:triage_id>', methods=['GET'])
@login_required
def delete_triage(triage_id):
    cur_triage = Triage.query.get(triage_id)
    if cur_triage:
        if cur_triage.user_id == current_user.id:
            db.session.delete(cur_triage)
            db.session.commit()

    return redirect(url_for('views.home'))
