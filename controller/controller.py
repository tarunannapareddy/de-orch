from flask import Flask, request, jsonify
from datetime import datetime
import psycopg2

app = Flask(__name__)

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

# Endpoint to register a workflow
@app.route('/register_workflow', methods=['POST'])
def register_workflow():
    data = request.json
    workflow_name = data.get('workflow_name')
    task_id = data.get('task_id')
    username = data.get('username')

    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("INSERT INTO workflow_definition (name, task_id, user) VALUES (%s, %s, %s)", 
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
            'username': workflow[3],
            'timestamp': str(workflow[4])  # Convert timestamp to string for JSON serialization
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

    cur.execute("INSERT INTO task_definition (name, user) VALUES (%s, %s);", 
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
            'username': task[2],
            'timestamp': str(task[3])  # Convert timestamp to string for JSON serialization
        }
        tasks_list.append(task_dict)

    return jsonify(tasks_list), 200

# Endpoint to start a workflow
@app.route('/start_workflow', methods=['POST'])
def start_workflow():
    # Implementation for starting a workflow
    pass

if __name__ == '__main__':
    app.run(debug=True)
