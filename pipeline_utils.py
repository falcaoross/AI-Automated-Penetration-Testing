import os
import json
import time
import signal

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATUS_FILE = os.path.join(BASE_DIR, ".pipeline_status.json")
LOG_PATH = os.path.join(BASE_DIR, "pipeline.log")

def load_status():
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r") as f:
                return json.load(f)
        except:
            return None
    return None

def save_status(stage, is_running=False, pid=None):
    status = {
        "stage": stage,
        "is_running": is_running,
        "pid": pid,
        "last_update": time.time()
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f)

def clear_status():
    if os.path.exists(STATUS_FILE):
        try:
            os.remove(STATUS_FILE)
        except:
            pass

def is_pid_running(pid):
    if pid is None: return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
