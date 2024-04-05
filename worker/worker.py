import redis
import json
import concurrent.futures
import base64

# Connect to Redis
redis_host = '127.0.0.1'
redis_port = 6379
redis_db = 0
r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# Sample tasks to add to the Redis list for testing
sample_tasks = [
    {'workflow_name': 'IMAGE_PROCESSING', 'task_name': 'BLUR_IMAGE', 'workflow_id': '1', 'task_id': '1', 'request': 'test.jpeg'},
    {'workflow_name': 'IMAGE_PROCESSING', 'task_name': 'ROTATE_IMAGE', 'workflow_id': '1', 'task_id': '1', 'request': 'test.jpeg'}
]

'''
# Add sample tasks to the Redis list
for task in sample_tasks:
    with open(task['request'], 'rb') as f:
        image_data = f.read()
        image_base64 = base64.b64encode(image_data).decode('utf-8')
        task['request'] = image_base64
        task_json = json.dumps(task)
        r.rpush('worker_queue', task_json)
'''

# Create Threadpool for executing tasks
executor = concurrent.futures.ThreadPoolExecutor()

def process_blur_image(image_data):
    image_data = base64.b64decode(image_data)
    with open('blur_image.jpg', 'wb') as f:
        f.write(image_data)
    print('Saved blur image')
    return image_data


def process_rotate_image(image_data):
    image_data = base64.b64decode(image_data)
    with open('rotate_image.jpg', 'wb') as f:
        f.write(image_data)
    print('Saved rotate image')
    return image_data

 #worker= {'workflow_exec_id': id ,'workflow_name': workflow_name, 'task_name': task_name, 'workflow_id': workflow_id, 'task_id': task_id, 'tasks_id': tasks_id, 'request': request}

# Function to continuously process tasks from Redis
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

        if task_name == 'BLUR_IMAGE':
            future = executor.submit(process_blur_image, request)
        elif task_name == 'ROTATE_IMAGE':
            future = executor.submit(process_rotate_image, request)
        else:
            print("Unknown task type:", task_name)
        print('got result')
        result = future.result() 
        print('got result', result)
        data = {'worker':{'data': 'worker', 'workflow_exec_id': workflow_exec_id ,'workflow_name': workflow_name, 'task_name': task_name, 'workflow_id': workflow_id, 'task_id': task_id, 'tasks_id': tasks_id, 'request': base64.b64encode(result).decode('utf-8')}}
        worker_json = json.dumps(data)
        r.rpush('scheduler_queue', worker_json)  

process_tasks()
