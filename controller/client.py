import requests

# Server URL
SERVER_URL = 'http://localhost:5000'  # Update with your server URL

# Sample data for workflow and task registration
workflow_data = {
    'workflow_name': 'Sample Workflow',
    'task_id': 1,
    'username': 'user1'
}

task_data = {
    'task_name': 'Sample Task',
    'username': 'user1'
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

# Function to get all tasks
def get_tasks():
    response = requests.get(f'{SERVER_URL}/tasks')
    print(response.json())

if __name__ == '__main__':
    # Ask the user which endpoint to hit
    print("Choose an endpoint to hit:")
    print("1. Register Workflow")
    print("2. Get Workflows")
    print("3. Register Task")
    print("4. Get Tasks")

    choice = input("Enter your choice: ")

    if choice == '1':
        register_workflow()
    elif choice == '2':
        get_workflows()
    elif choice == '3':
        register_task()
    elif choice == '4':
        get_tasks()
    else:
        print("Invalid choice")