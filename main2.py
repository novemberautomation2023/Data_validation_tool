import subprocess
import os
import sys
set_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print("set path", set_path)
sys.path.append(set_path)
subprocess.run("python Test_Validation/Contact_info_file2raw_validation.py", shell=True)
#subprocess.run("python Test_Validation/Contact_info_Raw2Bronze_validation.py", shell=True)
#subprocess.run("python Test_Validation/Contact_info_Bronze2Silver_validation.py", shell=True)
