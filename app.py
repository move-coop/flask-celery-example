import os
import random
import time
import json
from flask import Flask, request, render_template, session, flash, redirect, \
    url_for, jsonify
from celery import Celery
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


app = Flask(__name__)
app.config['SECRET_KEY'] = 'top-secret!'


# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

from parsons import Redshift
rs = Redshift()

# Initialize Celery
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


@app.route('/query', methods=['POST'])
def query():
    """Background task to send sql query"""

    sql = request.get_data()
    task = asyncquery.apply_async(args=[sql])
    flash(f'Submitted query. Task ID {task.id}, {task.kwargs}, SQL: {sql}')
    
    # Ideally this would return the task id so that we can poll for the status. See
    # taskstatus method for an example of how this could be done.
    return jsonify({}), 202, {'Location': url_for('taskstatus',
                                                  task_id=task.id)}


@celery.task(bind=True)
def asyncquery(self, sql):
    """ Run the celery task for a query """

    # Ideally, this would return a json object. There are issues with Celery
    # in that returned objects need to be JSON serializable.
    json_path = rs.query(sql).to_json()
    return json_path


@app.route('/', methods=['GET', 'POST'])
def index():

    if request.method == 'GET':
        return render_template('index.html', email=session.get('email', ''))

    # This method submits a query via a form which is probably not what we want 
    # to be doing.
    if request.form['submit'] == 'Run':
        sql = request.form['sql']
        task = asyncquery.delay(sql)
        flash(f'Submitted query. Task ID {task.id}, {task.kwargs}')

    return redirect(url_for('index'))

# This code was provided as part of the initial demo.

@app.route('/longtask', methods=['POST'])
def longtask():
    task = long_task.apply_async()
    return jsonify({}), 202, {'Location': url_for('taskstatus',
                                                  task_id=task.id)}

@celery.task(bind=True)
def long_task(self):
    """Background task that runs a long function with progress reports."""
    verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
    adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
    noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']
    message = ''
    total = random.randint(10, 50)
    for i in range(total):
        if not message or random.random() < 0.25:
            message = '{0} {1} {2}...'.format(random.choice(verb),
                                              random.choice(adjective),
                                              random.choice(noun))
        self.update_state(state='PROGRESS',
                          meta={'current': i, 'total': total,
                                'status': message})
        time.sleep(1)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 42}

@app.route('/status/<task_id>')
def taskstatus(task_id):
    task = long_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)
