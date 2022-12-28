from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
import requests
import pandas as pd
from io import StringIO

DATASET_URL = "https://www.fdic.gov/resources/resolutions/bank-failures/failed-bank-list/banklist.csv"

app = Flask(__name__)
USER = "harpreetathwal"
PW = ""
URL = "localhost"
DB = "postgres"
DB_URL = f"postgresql+psycopg2://{USER}:{PW}@{URL}/{DB}"
app.config['SQLALCHEMY_DATABASE_URI'] = DB_URL
db = SQLAlchemy(app)


# class User(db.Model):
#     id = db.Column(db.Integer, primary_key=True)
#     username = db.Column(db.String(80), unique=True, nullable=False)
#     email = db.Column(db.String(120), unique=True, nullable=False)

#     def __repr__(self):
#         return '<User %r>' % self.username

class Bank(db.Model):
    id = db.Column(db.INTEGER, primary_key=True, autoincrement=True)
    bank_name = db.Column(db.String(100))
    city = db.Column(db.String(100))
    state = db.Column(db.String(100))
    cert = db.Column(db.String(100))
    acquiring_institution = db.Column(db.String(100))
    closing_date = db.Column(db.String(100))
    fund = db.Column(db.String(100))

    def __repr__(self):
        return '<Bank %r>' % self.bank_name


def create_all(app):
    with app.app_context():
        db.create_all()
    return "success"

def list_tables(db):
    for t in db.metadata.sorted_tables:
        print(t.name)

# Returns list of model objects using url as datasource
def get_data(url=DATASET_URL):
    with requests.get(url) as resp:
        csv_data = StringIO(resp.text)
        df = pd.read_csv(csv_data)
        # Clean column names to match
        df.columns = [c.lower().strip().replace(" ","_") for c in  df.columns]
        new_rows = []
        for i,row in df.iterrows():
            new_rows.append(Bank(**(row.to_dict())))
    return new_rows


@app.route('/')
def hello():
    return render_template('index.html', banks=Bank.query.all(), url=DATASET_URL)


if __name__ == "__main__":
    print("Deleting all models")
    with app.app_context():
        Bank.__table__.drop(db.engine)
    print("Creating all models")
    print(create_all(app))
    print(list_tables(db))
    new_rows = get_data()
    with app.app_context():
        db.session.add_all(new_rows)
        db.session.commit()
    print(f"Success! Added {len(new_rows)} rows")
    app.run()
