import os
import sys
import redis
import json
import concurrent.futures
import base64
import numpy as np
from PIL import Image
import uuid
import grpc
from concurrent import futures
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
import workerServer.workerServer_pb2_grpc as workerServer_pb2_grpc
import workerServer.workerServer_pb2 as workerServer_pb2
import threading
import argparse 
import requests

# Connect to Redis
redis_host = '127.0.0.1'
redis_port = 6379
redis_db = 0
r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
data_map = {}

# Create Threadpool for executing tasks
executor = concurrent.futures.ThreadPoolExecutor()

def process_image(request, p, type):
    image = request.get('image')
    memId = request.get('memId')
    if image:
        image_data = base64.b64decode(image)
    else:
        image_data = data_map.pop(memId, None)
        if not image_data:
            print(f"Image data for ID {memId} not found.")
            return None
    with open(f'{p}_{memId}.jpg', 'wb') as f:
        f.write(image_data)
    if type == 'REGULAR':
        return {'image' : base64.b64encode(image_data).decode('utf-8')}
    result_id = str(uuid.uuid4())
    data_map[result_id] = image_data
    print(f'stored result :{result_id} for {p}')
    return {'memId' : result_id}

def process_tasks(queue, type):
    print(f'reading fom queue {queue} in mode {type}')
    while True:
        task_object = r.blpop(queue, timeout=0)
        _, task_json = task_object
        task_dict = json.loads(task_json)
        task_name = task_dict['task_name']
        request = task_dict['request']
        task_id = task_dict['task_id']
        workflow_name = task_dict['workflow_name']
        workflow_id = task_dict['workflow_id']
        workflow_exec_id = task_dict['workflow_exec_id']
        tasks_id = task_dict['tasks_id']
        worker_id=task_dict['worker_id']
        print('Received task', task_name, task_id, 'from workflow', workflow_name, workflow_id, workflow_exec_id)
        result_data = process_image(request, task_name, type)

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
                    'request': result_data,
                    'worker_id':worker_id
                }
            }
            worker_json = json.dumps(data)
            r.rpush('scheduler_queue', worker_json)
            print('Task completed:', task_name, task_id)

class WorkerServerServicer(workerServer_pb2_grpc.WorkerServicer):

    def transferData(self, ip, port, id):
        channel = grpc.insecure_channel(f'{ip}:{port}')
        stub = workerServer_pb2_grpc.WorkerStub(channel)
        request = workerServer_pb2.StoreDataInput(id = id, image_data = data_map.get(id))
        response = stub.storeDate(request)
        return response.status

    def migrate(self, request, context):
        status = self.transferData(request.ip, request.port, request.id)
        return workerServer_pb2.MigrationOutput(status= status)
    
    def storeDate(self, request, context):
        data_map[request.id] = request.image_data
        return workerServer_pb2.StoreDataOutput(status= True)

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    workerServer_pb2_grpc.add_WorkerServicer_to_server(WorkerServerServicer(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Server started. Listening on port {port}...")
    server.wait_for_termination()

def register_worker(ip, port, queue_name, pool_count):
    url = 'http://localhost:5000/register_worker'
    data = {
        'ip': ip,
        'port': port,
        'queue_name': queue_name,
        'pool_count': pool_count
    }
    response = requests.post(url, json=data)
    print(f"Register worker response: {response.json()}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=50052, help='Port number for the gRPC server')
    parser.add_argument('-q', '--queue', type=str, default='worker_queue', help='Redis queue name')
    parser.add_argument('-c', '--pool', type=int, default=1, help='Worker pool count')
    parser.add_argument('-t', '--type', type=str, default='REGULAR', help='Type of worker REGULAR/SHARED')
    args = parser.parse_args()

    register_worker('localhost', args.port, args.queue, args.pool)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.pool)
    for _ in range(args.pool):
        executor.submit(process_tasks, args.queue, args.type)
    serve_thread = threading.Thread(target=serve, args=(args.port,))
    serve_thread.start()
    serve_thread.join()
