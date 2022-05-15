
import time
import random
from datetime import datetime
from flask import Flask, Response

app = Flask(__name__)

# This Route Generates random computer data :

@app.route('/computer_data/<computer_id>')
def get_sensor_data(computer_id):
    
    timestamp = str(time.time())
    cpu_usage = str(int(random.uniform(0, 100)))
    memory_usage = str(int(random.uniform(0, 100)))
    disk_usage = str(int(random.uniform(0, 100)))
    network_usage = str(int(random.uniform(0, 100)))

    response = "{computer_id} {timestamp} {disk_usage} {network_usage} {cpu_usage} {memory_usage}\
                ".format(
        computer_id=computer_id,
        timestamp=timestamp,
        disk_usage=disk_usage,
        network_usage=network_usage,
        cpu_usage=cpu_usage,
        memory_usage=memory_usage,
    )

    return Response(response, mimetype='text/plain')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='3030')
