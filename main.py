from cmath import nan
import pymysql
from flask_table import Table, Col, LinkCol
from flask import flash, render_template, request, redirect, url_for
# from werkzeug import generate_password_hash, check_password_hash
from werkzeug.security import generate_password_hash, check_password_hash
from flask import Flask
from flaskext.mysql import MySQL
import os
from os.path import join, dirname, realpath
import pandas as pd
import numpy as np
import math
app = Flask(__name__)
app.secret_key = "secret key"

app.config["DEBUG"] = True
# Upload folder
UPLOAD_FOLDER = 'C:/Users/tranq/Downloads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


mysql = MySQL()
# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'admin'
app.config['MYSQL_DATABASE_PASSWORD'] = '12345678'
app.config['MYSQL_DATABASE_DB'] = 'data'
app.config['MYSQL_DATABASE_HOST'] = 'database-2.cywwdompfbdz.us-east-1.rds.amazonaws.com'
mysql.init_app(app)


class Results(Table):
    id = Col('id', show=False)
    rank_new = Col('rank_new')
    personName = Col('personName')
    age = Col('age')
    finalWorth = Col('finalWorth')
    category = Col('category')
    source = Col('source')
    country = Col('country')
    state = Col('state')
    city = Col('city')
    countryOfCitizenship = Col('countryOfCitizenship')
    organization = Col('organization')
    selfMade = Col('selfMade')
    gender = Col('gender')
    birthDate = Col('birthDate')
    title = Col('title')
    philanthropyScore = Col('philanthropyScore')
    residenceMsa = Col('residenceMsa')
    numberOfSiblings = Col('numberOfSiblings')
    bio = Col('bio')
    about = Col('about')
    edit = LinkCol('Edit', 'edit_view', url_kwargs=dict(id='id'))
    delete = LinkCol('Delete', 'delete_user', url_kwargs=dict(id='id'))


@app.route('/new_user')
def add_user_view():
    return render_template('add.html')


@app.route('/main')
def general():
    return render_template('./single.html')


@app.route('/us')
def us():
    return render_template('us.html')


@app.route('/india')
def india():
    return render_template('india.html')


@app.route('/china')
def china():
    return render_template('china.html')


@app.route('/add', methods=['POST'])
def uploadFiles():
    # get the uploaded file

    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        file_path = os.path.join(
            app.config['UPLOAD_FOLDER'], uploaded_file.filename)
        # set the file path
        uploaded_file.save(file_path)
        parseCSV(file_path)
        # save the file

        return redirect('/crud')


def parseCSV(filePath):
    # CVS Column Names
    try:
        col_names = ['rank_new', 'personName', 'age', 'finalWorth', 'category', 'source', 'country', 'state',
                     'city', 'countryOfCitizenship', 'organization', 'selfMade', 'gender', 'birthDate',
                     'title', 'philanthropyScore', 'residenceMsa', 'numberOfSiblings', 'bio', 'about']

        # Use Pandas to parse the CSV file
        csvData = pd.read_csv(filePath, names=col_names,
                              header=None, on_bad_lines='skip')

        # Loop through the Rows
        conn = mysql.connect()
        cursor = conn.cursor()
        j = 0
        for i, row in csvData.iterrows():
            if j != 0:
                sql = "INSERT INTO `forbes_2022_billionaires`(rank_new, personName, age, finalWorth, category, source, country, state, city,countryOfCitizenship, organization, selfMade, gender, birthDate, title, philanthropyScore, residenceMsa, numberOfSiblings, bio, about) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                value = (row['rank_new'], row['personName'], row['age'], row['finalWorth'], row['category'], row['source'], row['country'], row['state'], row['city'], row['countryOfCitizenship'],
                         row['organization'], row['selfMade'], row['gender'], row['birthDate'], row['title'], row['philanthropyScore'], row['residenceMsa'], row['numberOfSiblings'], row['bio'], row['about'])
                y = list(value)
                for i in range(len(y)):
                    if pd.isna(y[i]):
                        y[i] = ""
                valueNew = tuple(y)
                cursor.execute(sql, valueNew)
                conn.commit()
            j = j + 1

        print("Insert Data Successfully")

        flash('User added successfully!')
    except Exception as e:
        print('error', e)
        return 'error parse'
    finally:
        cursor.close()
        conn.close()


@app.route('/')
def MainPage():
    return render_template('single.html')


@app.route('/crud')
def users():
    try:
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        cursor.execute(
            "SELECT * FROM forbes_2022_billionaires LIMIT 10")
        rows = cursor.fetchall()

        table = Results(rows)
        table.border = True
        return render_template('users.html', table=table)
    except Exception as e:
        print(e)
        return "error"
    finally:
        cursor.close()
        conn.close()


@app.route('/edit/<int:id>')
def edit_view(id):
    try:
        conn = mysql.connect()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(
            "SELECT * FROM forbes_2022_billionaires WHERE id=%s", id)
        row = cursor.fetchone()
        if row:
            return render_template('edit.html', row=row)
        else:
            return 'Error loading #{id}'.format(id=id)
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/update', methods=['POST'])
def update_user():
    try:
        rank_new = request.form['rank_new']
        personName = request.form['personName']
        age = request.form['age']
        finalWorth = request.form['finalWorth']
        category = request.form['category']
        source = request.form['source']
        country = request.form['country']
        state = request.form['state']
        city = request.form['city']
        countryOfCitizenship = request.form['countryOfCitizenship']
        organization = request.form['organization']
        selfMade = request.form['selfMade']
        gender = request.form['gender']
        birthDate = request.form['birthDate']
        title = request.form['title']
        philanthropyScore = request.form['philanthropyScore']
        residenceMsa = request.form['residenceMsa']
        numberOfSiblings = request.form['numberOfSiblings']
        bio = request.form['bio']
        about = request.form['about']

        id = request.form['id']
        # validate the received values
        if request.method == 'POST':
            sql = "UPDATE forbes_2022_billionaires SET rank_new =%s, personName = %s,age = %s,finalWorth = %s,category=%s,source = %s,country = %s,state = %s,city = %s,countryOfCitizenship = %s,organization = %s,selfMade = %s,gender = %s,birthDate = %s,title = %s,philanthropyScore = %s,residenceMsa = %s,numberOfSiblings = %s, bio = %s,about = %s where id = %s"
            data = (rank_new, personName, age, finalWorth, category, source, country, state, city, countryOfCitizenship,
                    organization, selfMade, gender, birthDate, title, philanthropyScore, residenceMsa, numberOfSiblings, bio, about, id)
            conn = mysql.connect()
            cursor = conn.cursor()
            cursor.execute(sql, data)
            conn.commit()
            flash('User updated successfully!')
            return redirect('/crud')
        else:
            return 'Error while updating user'
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


@app.route('/delete/<int:id>')
def delete_user(id):
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM forbes_2022_billionaires WHERE id=%s", (id,))
        conn.commit()
        flash('User deleted successfully!')
        return redirect('/crud')
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5555)
