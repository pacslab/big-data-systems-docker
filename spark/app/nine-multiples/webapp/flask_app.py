"""
    This Flask web app provides a very simple dashboard to visualize the statistics sent by the spark app.
    The web app is listening on port 5000.
    All apps are designed to be run in Docker containers.
    
    Made for: EECS 4415 - Big Data Systems (Department of Electrical Engineering and Computer Science, York University)
    Author: Changyuan Lin

"""


from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)

@app.route('/updateData', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)
    data = r.get('data')
    try:
        data = json.loads(data)
    except TypeError:
        return "waiting for data..."
    try:
        yes_index = data['isMultipleOf9'].index('Yes')
        isMultipleOf9 = data['count'][yes_index]
    except ValueError:
        isMultipleOf9 = 0
    try:
        notMultipleOf9 = data['count'][1 - yes_index]
    except NameError:
        notMultipleOf9 = 0
    x = [1, 2]
    height = [isMultipleOf9, notMultipleOf9]
    tick_label = ['isMultipleOf9', 'notMultipleOf9']
    plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:orange', 'tab:blue'])
    plt.ylabel('Count')
    plt.xlabel('Type')
    plt.title('Distribution of numbers')
    plt.savefig('/app/nine-multiples/webapp/static/images/chart.png')
    return render_template('index.html', url='/static/images/chart.png', isMultipleOf9=isMultipleOf9, notMultipleOf9=notMultipleOf9)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
