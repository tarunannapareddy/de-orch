import requests
import base64

# Server URL
SERVER_URL = 'http://localhost:5000'  # Update with your server URL

# Helper functions for image processing
def process_image(path_img):
    with open(path_img, 'rb') as f:
        image_data = f.read()
        image_base64 = base64.b64encode(image_data).decode('utf-8') 
        
        return image_base64

# Get image data
path = 'test.jpeg'
image = process_image(path)

# Sample data for workflow and task registration
workflow_data = {
    'workflow_name': 'Image Workflow',
    'task_id': [3,4],
    'username': 'user1'
}

task_data = {
    'task_name': 'BLUR_IMAGE',
    'username': 'user1'
}

task_data2 = {
    'task_name': 'ROTATE_IMAGE',
    'username': 'user1'
}

exec_data = {
    'workflow_id': 6,
    'request': {
        'image' : image
    }
}

# Function to register a workflow
def register_workflow():
    response = requests.post(f'{SERVER_URL}/register_workflow', json=workflow_data)
    print(response.json())

# Function to get all workflows
def get_workflows():
    response = requests.get(f'{SERVER_URL}/workflows')
    print(response.json())

# Function to register a task
def register_task():
    response = requests.post(f'{SERVER_URL}/register_task', json=task_data)
    print(response.json())
    response = requests.post(f'{SERVER_URL}/register_task', json=task_data2)
    print(response.json())

# Function to get all tasks
def get_tasks():
    response = requests.get(f'{SERVER_URL}/tasks')
    print(response.json())

def start_workflow():
    response = requests.post(f'{SERVER_URL}/start_workflow', json=exec_data)
    print(response.json())

def get_exec():
    response = requests.get(f'{SERVER_URL}/get_exec')
    print(response.json())

if __name__ == '__main__':
    # Ask the user which endpoint to hit
    print("Choose an endpoint to hit:")
    print("1. Register Workflow")
    print("2. Get Workflows")
    print("3. Register Task")
    print("4. Get Tasks")
    print("5. Start Workflow")
    print("6. Get executions")

    choice = input("Enter your choice: ")

    if choice == '1':
        register_workflow()
    elif choice == '2':
        get_workflows()
    elif choice == '3':
        register_task()
    elif choice == '4':
        get_tasks()
    elif choice == '5':
        start_workflow()
    elif choice == '6':
        get_exec()
    else:
        print("Invalid choice")