import redis
import json
import concurrent.futures
import base64
import numpy as np
from PIL import Image
import uuid

# Connect to Redis
redis_host = '127.0.0.1'
redis_port = 6379
redis_db = 0
r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
data_map = {}

# Create Threadpool for executing tasks
executor = concurrent.futures.ThreadPoolExecutor()

def process_blur_image(request):
    image = request.get('image')
    memId = request.get('memId')
    if image:
        image_data = base64.b64decode(image)
    else:
        image_data = data_map.pop(memId, None)
        if not image_data:
            print(f"Image data for ID {memId} not found.")
            return None
    with open('blur_image.jpg', 'wb') as f:
        f.write(image_data)
    result_id = str(uuid.uuid4())
    data_map[result_id] = image_data
    return result_id

def process_rotate_image(request):
    image = request.get('image')
    memId = request.get('memId')
    if image:
        image_data = base64.b64decode(image)
    else:
        image_data = data_map.pop(memId,None)
        if not image_data:
            print(f"Image data for ID {memId} not found.")
            return None
    with open('rotate_image.jpg', 'wb') as f:
        f.write(image_data)
    result_id = str(uuid.uuid4())
    data_map[result_id] = image_data
    return result_id

def process_tasks():
    while True:
        task_object = r.blpop('worker_queue', timeout=0)
        _, task_json = task_object
        task_dict = json.loads(task_json)
        task_name = task_dict['task_name']
        request = task_dict['request']
        task_id = task_dict['task_id']
        workflow_name = task_dict['workflow_name']
        workflow_id = task_dict['workflow_id']
        workflow_exec_id = task_dict['workflow_exec_id']
        tasks_id = task_dict['tasks_id']
        print('Received task', task_name, task_id, 'from workflow', workflow_name, workflow_id, workflow_exec_id)
        result_data = None
        if task_name == 'BLUR_IMAGE':
            result_data = process_blur_image(request)
        elif task_name == 'ROTATE_IMAGE':
            result_data = process_rotate_image(request)
        else:
            print("Unknown task type:", task_name)
        if result_data:
            data = {
                'worker': {
                    'data': 'worker',
                    'workflow_exec_id': workflow_exec_id,
                    'workflow_name': workflow_name,
                    'task_name': task_name,
                    'workflow_id': workflow_id,
                    'task_id': task_id,
                    'tasks_id': tasks_id,
                    'request': {
                        'memId': result_data
                    }
                }
            }
            worker_json = json.dumps(data)
            r.rpush('scheduler_queue', worker_json)
            print('Task completed:', task_name, task_id)

process_tasks()