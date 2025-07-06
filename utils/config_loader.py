
import json

def load_runtime_config():
    with open("runtime_config.json", "r") as f:
        return json.load(f)
