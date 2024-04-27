from flask import Flask, request, jsonify
from datetime import datetime
import psycopg2
import json
import redis
import time

app = Flask(__name__)


# Connect to Redis
redis_host = 'localhost'
redis_port = 6379
redis_db = 0
r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
try:
    # Perform a simple operation to check connection
    r.ping()
    print("Connected to Redis successfully!")
except redis.exceptions.ConnectionError:
    print("Error: Failed to connect to Redis")

# Function to connect to PostgreSQL database
def connect_to_db():
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgresuser",
        password="postgres",
        host="35.230.178.196",
        port="5432"
    )
    return conn

# Helper functions
def get_task_id(workflow_id):
    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("SELECT task_id FROM workflow_definition where id=%s", str(workflow_id))
    task_ids = cur.fetchone()
    print(task_ids)

    cur.close()
    conn.close()
    
    return task_ids[0][0]

# Endpoint to register a workflow
@app.route('/register_workflow', methods=['POST'])
def register_workflow():
    data = request.json
    workflow_name = data.get('workflow_name')
    task_id = data.get('task_id')
    username = data.get('username')

    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("INSERT INTO workflow_definition (name, task_id, user_info) VALUES (%s, %s, %s)", 
                (workflow_name, task_id, username))
    
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"message": "Workflow registered successfully"}), 201

# Endpoint to get all workflows
@app.route('/workflows', methods=['GET'])
def get_workflows():
    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM workflow_definition")
    workflows = cur.fetchall()

    cur.close()
    conn.close()

    # Convert workflows to a list of dictionaries for JSON serialization
    workflows_list = []
    for workflow in workflows:
        workflow_dict = {
            'workflow_id': workflow[0],
            'workflow_name': workflow[1],
            'task_id': workflow[2],
            'timestamp': str(workflow[3]),  # Convert timestamp to string for JSON serialization
            'username': workflow[4]
        }
        workflows_list.append(workflow_dict)

    return jsonify(workflows_list), 200

# Endpoint to register a task
@app.route('/register_task', methods=['POST'])
def register_task():
    data = request.json
    task_name = data.get('task_name')
    username = data.get('username')

    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("INSERT INTO task_definition (name, user_info) VALUES (%s, %s);", 
                (task_name, username))
    
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"message": "Task registered successfully"}), 201

# Endpoint to get all tasks
@app.route('/tasks', methods=['GET'])
def get_tasks():
    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM task_definition")
    tasks = cur.fetchall()

    cur.close()
    conn.close()

    # Convert tasks to a list of dictionaries for JSON serialization
    tasks_list = []
    for task in tasks:
        task_dict = {
            'task_id': task[0],
            'task_name': task[1],
            'timestamp': str(task[2]),  # Convert timestamp to string for JSON serialization
            'username': task[3]
        }
        tasks_list.append(task_dict)

    return jsonify(tasks_list), 200

# Endpoint to start a workflow
@app.route('/start_workflow', methods=['POST'])
def start_workflow():
    data = request.json
    workflow_id = data.get('workflow_id')
    image = data.get('request')
    img_json = json.dumps(image)
    task_id = get_task_id(workflow_id)
    print(task_id)

    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("INSERT INTO workflow_execution (workflow_id, next_task_id, request, status) VALUES (%s, %s, %s, 'STARTED') RETURNING id;", 
                (workflow_id, task_id, img_json))
    latest_id = cur.fetchone()[0]
    print(latest_id)
    conn.commit()
    cur.close()
    conn.close()

    time.sleep(2)

    data = {'controller':{'workflow_exe_id': str(latest_id)}}
    # Serialize the dictionary into a JSON string
    data_json = json.dumps(data)
    r.rpush('scheduler_queue', data_json)
    

    return jsonify({"message": "Task registered successfully"}), 201

# Endpoint to start a workflow
@app.route('/get_exec', methods=['GET'])
def get_exec():
    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("SELECT id, workflow_id, next_task_id, status FROM workflow_execution")
    tasks = cur.fetchall()

    cur.close()
    conn.close()

    # Convert tasks to a list of dictionaries for JSON serialization
    tasks_list = []
    for task in tasks:
        task_dict = {
            'Execution ID': task[0],
            'Workflow ID': task[1],
            'Task ID': task[2],  # Convert timestamp to string for JSON serialization
            'STATUS': task[3]
        }
        tasks_list.append(task_dict)

    return jsonify(tasks_list), 200

@app.route('/register_worker', methods=['POST'])
def reg_worker():
    data = request.json
    worker_ip = data.get('ip')
    worker_port = data.get('port')
    queue_name = data.get('queue_name')
    pool_count = data.get('pool_count')

    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("INSERT INTO workers (ip, port, queue_name, pool_count) VALUES (%s, %s, %s, %s)", 
                (worker_ip, worker_port, queue_name, pool_count))
    
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"message": "Worker registered successfully"}), 201

if __name__ == '__main__':
    app.run(debug=True)
