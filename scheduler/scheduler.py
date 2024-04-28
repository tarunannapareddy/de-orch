import os
import sys
import redis
import psycopg2
import json
import base64
import time
import grpc
import openpyxl
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
import workerServer.workerServer_pb2_grpc as workerServer_pb2_grpc
import workerServer.workerServer_pb2 as workerServer_pb2

# Global variable to store the last push time
last_push_time = None

workbook = openpyxl.Workbook()
sheet = workbook.active
sheet.append(["Time"])

# Connect to Redis
redis_host = '127.0.0.1'
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
    global last_push_time  # Access the global variable
    with open("test.jpeg", 'rb') as f:
        image_data = f.read()
        image_base64 = base64.b64encode(image_data).decode('utf-8')
        request = json.dumps(image_base64)
    data = {'worker':{'workflow_exec_id': "3" ,'workflow_name': "Sample Workflow", 'task_name': "Sample Task", 'workflow_id': "3", 'task_id': "2", 'tasks_id': [1,2], 'request': request}}
    # Serialize the dictionary into a JSON string
    data_json = json.dumps(data)
    r.rpush('scheduler_queue',  data_json)
    last_push_time = time.time()  # Update the last push time

# Function to continuously process tasks from Redis
def process_tasks():
    global last_push_time 
    while True:
        task_object = r.blpop('scheduler_queue', timeout=0)
        key, task_json = task_object
        # Parse the JSON string back into a dictionary
        data = json.loads(task_json)
        
        # Calculate and print time difference if last_push_time is not None
        if last_push_time is not None:
            current_time = time.time()
            time_difference = current_time - last_push_time
            print("processing time is :", time_difference, "event is", data)
            sheet.append([time_difference])
            workbook.save("processing_times.xlsx")
        
        if "controller" in data:
            print("task",data["controller"])
            wf= data["controller"]
            id= wf["workflow_exe_id"]
            print("id",id)
            cur = conn.cursor()

            # Get the worker with the least work
            worker_info={}
            wi={}
            cur.execute("SELECT * FROM workers")
            workers_e = cur.fetchall()
            for worker in workers_e:
                wid=worker[0]
                queue=worker[3]
                current_load=worker[5]
                work_load=int(worker[4])-int(worker[5])
                worker_info[wid] = {'queue': queue, 'load': work_load, 'current_load':current_load}
                wi[wid]=work_load
            sorted_dict = dict(sorted(wi.items(), key=lambda item: item[1]))
            sorted_items = list(sorted_dict.items())
            # Get the last item, which is a tuple containing the last key-value pair
            worker_id, _ = sorted_items[-1]
            worker_info[worker_id]['current_load']=worker_info[worker_id]['current_load']+1

            cur.execute("UPDATE workers SET current_pool = %s WHERE worker_id = %s", (worker_info[worker_id]['current_load'], worker_id))
            # Commit the transaction
            conn.commit()

            cur.execute("SELECT * FROM workflow_execution where id=%s",(id,))
            workflows_e = cur.fetchone()
            id=workflows_e[0]
            workflow_id=str(workflows_e[1])
            task_id=str(workflows_e[2])
            request=workflows_e[3]
            print("wf=",workflow_id,task_id)
            
            cur.execute("SELECT * FROM workflow_definition where id=%s",(workflow_id,))
            workflows_d = cur.fetchall()
            for workflow in workflows_d:
                workflow_name=workflow[1]
                tasks_id=workflow[2]
            print(tasks_id)
            cur.execute("SELECT * FROM task_definition where id=%s",(task_id,))
            task_d = cur.fetchall()
            for task in task_d:
                task_name=task[1]
            cur.close()
            print(task_name)
            #new addition worker_id
            worker= {'workflow_exec_id': id ,'workflow_name': workflow_name, 'task_name': task_name, 'workflow_id': workflow_id, 'task_id': task_id, 'tasks_id': tasks_id, 'request': request, 'worker_id':worker_id}
            worker_json = json.dumps(worker)
            last_push_time = time.time()
            r.rpush(worker_info[worker_id]['queue'],  worker_json)  

        if "worker" in data:
            wf= data["worker"]
            workflow_exec_id= wf["workflow_exec_id"]
            workflow_id=wf["workflow_id"]
            workflow_name=wf["workflow_name"]
            task_id=int(wf["task_id"])
            tasks_id=wf["tasks_id"]
            request=wf["request"]
            worker_id=wf["worker_id"]
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
                cur = conn.cursor()
                worker_info={}
                wi={}
                cur.execute("SELECT * FROM workers")
                workers_e = cur.fetchall()
                for worker in workers_e:
                    id=worker[0]
                    ip=worker[1]
                    port=worker[2]
                    queue=worker[3]
                    current_load=worker[5]
                    work_load=int(worker[4])-int(worker[5])
                    worker_info[id] = {'queue': queue, 'load': work_load, 'current_load':current_load, 'ip':ip,'port':port}
                    wi[id]=work_load
                
                worker_info[worker_id]['current_load']=worker_info[worker_id]['current_load']-1
                wi[worker_id]=wi[worker_id]+1
                cur.execute("UPDATE workers SET current_pool = %s WHERE worker_id = %s", (worker_info[worker_id]['current_load'], worker_id))
                # Commit the transaction
                conn.commit()
                sorted_dict = dict(sorted(wi.items(), key=lambda item: item[1]))
                sorted_items = list(sorted_dict.items())
                # Get the last item, which is a tuple containing the last key-value pair
                new_worker_id, _ = sorted_items[-1]
                if new_worker_id==worker_id:
                    new_worker_id,_=sorted_items[-2]
                
                index = tasks_id.index(task_id)
                task_id=str(tasks_id[index+1])
                print(task_id)
                
                cur.execute("SELECT * FROM task_definition where id=%s",task_id)
                task_d = cur.fetchall()
                for task in task_d:
                    task_name=task[1]
                
                cur.execute("UPDATE workflow_execution SET next_task_id = %s, request = %s, status= 'INPROGRESS' WHERE id = %s", (str(task_id), json.dumps(request), workflow_exec_id))
    
                # Commit the transaction
                conn.commit()
                cur.close()

                while(1):
                    #what is the id value? hwo to fetch it?
                    channel = grpc.insecure_channel(f'{worker_info[worker_id]['ip']}:{worker_info[worker_id]['port']}')
                    stub = workerServer_pb2_grpc.WorkerStub(channel)
                    req = workerServer_pb2.MigrationInput(ip = worker_info[new_worker_id]['ip'], port = worker_info[new_worker_id]['port'], id=request["memId"])
                    response = stub.migrate(req)
                    if response.status:
                        break
                
                wdata= {'workflow_exec_id': workflow_exec_id ,'workflow_name': workflow_name, 'task_name': task_name, 'workflow_id': workflow_id, 'task_id': task_id, 'tasks_id': tasks_id, 'request': request, 'worker_id':new_worker_id}
                worker_json = json.dumps(wdata)
                last_push_time = time.time()
                r.rpush(worker_info[new_worker_id]['queue'],  worker_json)  

#pre_process_tasks()
process_tasks()
