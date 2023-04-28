import os
import sys
#from numpy.lib.shape_base import expand_dims
import pickle
from cbd import cbd
from flask import (
    Flask, flash, 
    g,
    request, redirect, render_template, 
    send_from_directory, session,
    url_for 
)
from werkzeug.utils import secure_filename


UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = ['xlsx', 'csv']

app = Flask(__name__, template_folder='templates')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = 'the random string of wazo'


# pull out all users from password file
PASSWORD_FILE = 'passwd'       # password file name
passwd = {}

def pullout_password(path, filename):
    try:
        f = open(os.path.join(path, filename), 'rb')
        passwd = pickle.load(f)
    except:
        passwd = {}
    else:
        f.close()

    return passwd

def update_password(path, filename, passwd):
    with open(os.path.join(path, filename), 'wb') as f:
        pickle.dump(passwd, f)
 

@app.before_request
def before_request():
    global passwd

    g.passwd = passwd = pullout_password(app.config['UPLOAD_FOLDER'],  PASSWORD_FILE)
    app.logger.info(f'{len(passwd)} of users in the system')

#users.append(User(username='wazo', password='password'))

    if 'username' in session:
        if session["username"] in passwd:
            g.login = True
            app.logger.info(f'{session["username"]} found in session')
        else:
            g.login = False
            app.logger.warning(f'{session["username"]} not found in session')
            session.pop('username', None)
    else:
        g.login = False

from wtforms import (
    BooleanField, 
    Form, 
    PasswordField, 
    SelectField, StringField, 
    validators
)

class RegistrationForm(Form):
    username = StringField('Username', [
        validators.DataRequired(),
        validators.Length(min=4, max=25)
    ], render_kw={"class": "input", "placeholder": "Username"})
    password = PasswordField('New Password', 
        [ validators.DataRequired(),
        validators.EqualTo('confirm', message='Passwords must match')
        ], render_kw={"class": "input", "placeholder": "Password"})
    confirm = PasswordField('Repeat Password', 
        render_kw={"class": "input", "placeholder": "Password"})

@app.route('/register', methods=['GET', 'POST'])
def register():
    form = RegistrationForm(request.form)
    if request.method == 'POST' and form.validate():
        if form.username.data in passwd:
            app.logger.warning(f'{form.username.data} already registered')
            return redirect(url_for('register'))

        passwd[form.username.data] = form.password.data
        update_password(app.config['UPLOAD_FOLDER'],  PASSWORD_FILE, passwd)
        app.logger.info('Thanks for registering')

        session['username'] = form.username.data
        return redirect(url_for('login'))

    return render_template('register.html', form=form)

class PasswordChangeForm(Form):
    username = SelectField('Username')
    password = PasswordField('New Password', 
        [ validators.DataRequired(),
        validators.EqualTo('confirm', message='Passwords must match')
        ], render_kw={"class": "input", "placeholder": "Password"})
    confirm = PasswordField('Repeat Password', 
        render_kw={"class": "input", "placeholder": "Password"})
@app.route('/change_password', methods=['GET', 'POST'])
def change_password():
    global passwd

    app.logger.info(f'{g.login}')
    if not g.login:
        app.logger.warning('not login yet')
        return redirect(url_for('login'))
    if session['username'] != 'admin':
        app.logger.warning('must be admin in order to change password')
        return redirect(url_for('login'))

    form = PasswordChangeForm(request.form)
    form.username.choices = [(username, username) for username in passwd.keys()]
    if request.method == 'POST' and form.validate():
        app.logger.info(f'username={form.username.data}, password={form.password.data}')
        passwd[form.username.data] = form.password.data
        update_password(app.config['UPLOAD_FOLDER'],  PASSWORD_FILE, passwd)
        app.logger.info(f"{form.username.data}'s password changed")
        session['username'] = form.username.data
        return redirect(url_for('login'))
    return render_template('change_password.html', form=form)

def allowed_file(filename):
    global ALLOWED_EXTENSIONS
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/login', methods=['GET', 'POST'])
def login():
    if not passwd or len(passwd) == 0:   # no user registered, go to register page
        return redirect(url_for('register'))
    if request.method == 'POST':
        session.pop('username', None)

        username = request.form['username']
        password = request.form['password']
        
        try:
            app.logger.info(f'username={username} password={password}')
            app.logger.info(f'{passwd[username]}')

            if passwd[username] == password and username == 'wazo':
                app.logger.info('continue to upload order')
                session['username'] = username
                g.login = True
                return render_template('home.html')
                #return redirect(url_for('upload_file'))

            if passwd[username] == password and username == 'admin':
                app.logger.info('continue to change password')
                session['username'] = username
                g.login = True
                return redirect(url_for('change_password'))
        except:
            pass

        return redirect(url_for('login'))

    return render_template('login.html')

def get_upload_files(filetags):
    files = []
    for f in filetags:
        if f not in request.files:
            app.logger.error('No '+ f +' part')
            files.append(None)
            continue

        file = request.files[f]
        if file.filename == '':
            app.logger.warning('No selected file')
            files.append(None)
            continue

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            app.logger.info('uploading '+filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            files.append(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        else:
            files.append(None)

    return files

import functools

def dec_wazofunc(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        log = open(os.path.join(app.config['UPLOAD_FOLDER'],  'wazo.log'), 'w')
        sys.stdout = log
        orig_stdout = sys.stdout
        outcome = func(*args, **kwargs)

        log.close()
        sys.stdout = orig_stdout
        return outcome
    return wrapper_decorator


@app.route('/container', methods=['GET', 'POST'])
@dec_wazofunc
def upload_file():
    app.logger.info(f'{g.login}')
    if not g.login:
        app.logger.warning('not login yet')
        return redirect(url_for('login'))
    if session['username'] != 'wazo':
        app.logger.warning('must be wazo in order to proceed yet')
        return redirect(url_for('login'))

    if request.method == 'POST':
        # check if the post request has the file part
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        order_file, cmb_file = get_upload_files(['order_file', 'cmb_file'])
        if not order_file or not cmb_file:
            return redirect(request.url)

        app.logger.info('cmb file:' + cmb_file)
        app.logger.info('order file:' + order_file)

        CONTAINER_SIZE = float(request.form['container'])
        app.logger.info(f'container_size={CONTAINER_SIZE}')

        filename = cbd(order_file, cmb_file, CONTAINER_SIZE)

        return redirect(url_for('downloaded_file', filename=filename))
    return render_template('upload_replenish_order.html')

from presales import wazo_presale, zip_files 
@app.route('/presales', methods=['GET', 'POST'])
@dec_wazofunc
def presales():
    app.logger.info(f'{g.login}')
    if not g.login:
        app.logger.warning('not login yet')
        return redirect(url_for('login'))
    if session['username'] != 'wazo':
        app.logger.warning('must be wazo in order to proceed yet')
        return redirect(url_for('login'))

    if request.method == 'POST':
        # check if the post request has the file part
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        threshold = int(request.form['threshold'])
        #presale_percentage = float(request.form['presale_percentage']) / 100.0
        order_file, inventory_file, cmb_file, rules_file  = get_upload_files(
                ['order_file', 'inventory_file', 'cmb_file', 'rules_file'])
        #if not order_file or not inventory_file or not rules_file:
        if not order_file or not inventory_file:
            return redirect(request.url)

        if cmb_file:
            app.logger.info('cmb file:' + cmb_file)
        else:
            app.logger.info('cmb file is NULL')

        app.logger.info(f'inventory threshold: {threshold}')
        app.logger.info('order file:' + order_file)
        app.logger.info('inventory file:' + inventory_file)
        files = wazo_presale(order_file, inventory_file, rules_file, cmb_file, threshold)

        filename = zip_files(files)
        return redirect(url_for('downloaded_file', filename=filename))
    return render_template('upload_purchase_order_detail.html')

@app.route('/uploads/<filename>')
def downloaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)

@app.route('/')
def home():
    return render_template('home.html')

if __name__ == '__main__':

    app.run(debug=True, host='0.0.0.0', port=5000)
