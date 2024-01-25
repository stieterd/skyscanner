from flask import Blueprint, render_template, request, flash, redirect, url_for
from .models import User
from werkzeug.security import generate_password_hash, check_password_hash
from . import db
from flask_login import login_user, login_required, logout_user, current_user

auth = Blueprint('auth', __name__)

KEY = "KAR_bgy381n3z"

@auth.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == "POST":
        email = request.form.get('email')
        password = request.form.get('password')

        user = User.query.filter_by(email=email).first()
        if user:
            if check_password_hash(user.password, password):
                flash('U bent ingelogd!', category='succes')
                login_user(user, remember=True)
                return redirect(url_for('views.home'))
            else:
                flash('Incorrect wachtwoord', category='error')
        else:
            flash('Incorrect E-mailadres', category='error')
    return render_template("login.html", user=current_user)

@auth.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))

@auth.route('sign-up', methods=['GET', 'POST'])
def sign_up():
    if request.method == 'POST':
        email = request.form.get('email')
        password1 = request.form.get("password1")
        password2 = request.form.get("password2")
        key = request.form.get("key")

        user = User.query.filter_by(email=email).first()
        if len(key) < 1:
            flash('Vul uw activeringscode in', category='error')
        elif key != KEY:
            flash('De activeringscode is onjuist', category='error')
        elif user:
            flash('Er is al een ander account gekoppeld aan dit e-mailadres', category='error')
        elif len(password1) < 7:
            flash('Wachtwoord moet uit minstens 7 tekens bestaan.', category='error')
        elif password1 != password2:
            flash('Wachtwoorden komen niet overeen', category='error')
        elif len(email) < 1:
            flash('Vul uw e-mailadres in', category='error')
        else:
            new_user = User(email=email, password=generate_password_hash(password1))
            db.session.add(new_user)
            db.session.commit()
            flash('Account aangemaakt!', category='success')
            return redirect(url_for('auth.login'))

    return render_template("sign_up.html", user=current_user)