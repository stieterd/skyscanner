from website import create_app
from website import db
import time
import datetime

app = create_app()

@app.template_filter('ctime')
def timectime(s):

    return time.ctime(float(s)) # datetime.datetime.fromtimestamp(s)


if __name__ == "__main__":

    with app.app_context():
        db.create_all()

    app.run(host="0.0.0.0", port=8000, debug=True) #zet debug uit wanneer website werkt

