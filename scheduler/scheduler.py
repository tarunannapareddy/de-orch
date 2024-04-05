import redis
import psycopg2
import json
import base64

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


conn = psycopg2.connect(
        dbname="postgres",
        user="postgresuser",
        password="postgres",
        host="35.230.178.196",
        port="5432"
    )

def pre_process_tasks():
    with open("test.jpeg", 'rb') as f:
        image_data = f.read()
        image_base64 = base64.b64encode(image_data).decode('utf-8')
        request = json.dumps(image_base64)
    data = {'worker':{'workflow_exec_id': "3" ,'workflow_name': "Sample Workflow", 'task_name': "Sample Task", 'workflow_id': "3", 'task_id': "2", 'tasks_id': [1,2], 'request': request}}
    # Serialize the dictionary into a JSON string
    data_json = json.dumps(data)
    r.rpush('scheduler_queue',  data_json)   

# Function to continuously process tasks from Redis
def process_tasks():
    while True:
        task_object = r.blpop('scheduler_queue', timeout=0)
        key, task_json = task_object
        # Parse the JSON string back into a dictionary
        data = json.loads(task_json)
        if "controller" in data:
            print("task",data["controller"])
            wf= data["controller"]
            id= wf["workflow_exe_id"]
            print("id",id)
            cur = conn.cursor()

            cur.execute("SELECT * FROM workflow_execution where id=%s",id)
            workflows_e = cur.fetchall()
            
            #{'workflow_name': 'IMAGE_PROCESSING', 'task_name': 'BLUR_IMAGE', 'workflow_id': '1', 'task_id': '1', 'request': 'test.jpeg'}
            # Convert workflows to a list of dictionaries for JSON serialization
            
            for workflow in workflows_e:
                id=workflow[0]
                workflow_id=str(workflow[1])
                task_id=str(workflow[2])
                request=workflow[3]
                print("wf=",workflow_id,task_id)
            
            cur.execute("SELECT * FROM workflow_definition where id=%s",workflow_id)
            workflows_d = cur.fetchall()
            for workflow in workflows_d:
                workflow_name=workflow[1]
                tasks_id=workflow[2]
            print(tasks_id)
            cur.execute("SELECT * FROM task_definition where id=%s",task_id)
            task_d = cur.fetchall()
            for task in task_d:
                task_name=task[1]
            cur.close()
            print(task_name)
            worker= {'workflow_exec_id': id ,'workflow_name': workflow_name, 'task_name': task_name, 'workflow_id': workflow_id, 'task_id': task_id, 'tasks_id': tasks_id, 'request': request}
            worker_json = json.dumps(worker)
            r.rpush('worker_queue',  worker_json)  

        if "worker" in data:
            wf= data["worker"]
            workflow_exec_id= wf["workflow_exec_id"]
            workflow_id=wf["workflow_id"]
            workflow_name=wf["workflow_name"]
            task_id=int(wf["task_id"])
            tasks_id=wf["tasks_id"]
            request=wf["request"]
            print(task_id, tasks_id)
            if tasks_id[len(tasks_id)-1]==task_id:
                #This means that all the task is completed
                cur = conn.cursor()
                #doubt what is the predefined status value
                cur.execute("UPDATE workflow_execution SET status = %s WHERE id = %s", ('COMPLETED', workflow_exec_id))
    
                # Commit the transaction
                conn.commit()
                cur.close()
            else:
                index = tasks_id.index(task_id)
                task_id=str(tasks_id[index+1])
                print(task_id)
                cur = conn.cursor()
                cur.execute("SELECT * FROM task_definition where id=%s",task_id)
                task_d = cur.fetchall()
                for task in task_d:
                    task_name=task[1]
                
                cur.execute("UPDATE workflow_execution SET next_task_id = %s, request = %s, status= 'INPROGRESS' WHERE id = %s", (str(task_id), request, workflow_exec_id))
    
                # Commit the transaction
                conn.commit()
                cur.close()

                worker= {'workflow_exec_id': workflow_exec_id ,'workflow_name': workflow_name, 'task_name': task_name, 'workflow_id': workflow_id, 'task_id': task_id, 'tasks_id': tasks_id, 'request': request}
                worker_json = json.dumps(worker)
                r.rpush('worker_queue',  worker_json)  

#pre_process_tasks()
process_tasks()