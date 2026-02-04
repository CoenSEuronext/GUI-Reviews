#run.py
import subprocess
import sys
import os

# Define the paths to the scripts
app_script = os.path.join(os.path.dirname(__file__), 'app.py')
worker_script = os.path.join(os.path.dirname(__file__), 'worker.py')

def start_services():
    # Start worker.py in a subprocess
    worker_process = subprocess.Popen([sys.executable, worker_script])

    # Start app.py in a subprocess (Flask app)
    app_process = subprocess.Popen([sys.executable, app_script])

    try:
        # Wait for the Flask app to terminate
        app_process.wait()
    except KeyboardInterrupt:
        print("\nShutting down both worker and Flask app...")

        # Kill the worker process if Flask app is interrupted
        worker_process.terminate()
        worker_process.wait()

        # Kill the Flask app
        app_process.terminate()
        app_process.wait()

if __name__ == "__main__":
    start_services()
