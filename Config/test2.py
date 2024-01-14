import os
import json
script_dir = os.path.dirname(__file__)
print(script_dir)

with open('Config/config.json','r') as f:
    config_file_data = json.loads(f.read())