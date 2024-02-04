from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from os import path
from flask_login import LoginManager

db = SQLAlchemy()
DB_NAME = "database"

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = '\x15\xcf\xda\x9e\xa1)\x0f\xe5_\x86\xf4p"\xcbH\x81\xbd\xd6q\xc3\x95\xa2b\x86'
    app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql://stieterd:Walvis12@localhost/{DB_NAME}'
    db.init_app(app)

    from .views import views
    from .auth import auth

    app.register_blueprint(views, url_prefix='/')
    app.register_blueprint(auth, url_prefix='/')

    from .models import User, Triage
    with app.app_context():
        db.create_all()

    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(id):
        return User.query.get(int(id))

    return app
