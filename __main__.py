import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import subprocess
from config.install.__main__ import RosieInstaller

if __name__ == "__main__":
    rosie = RosieInstaller()
    rosie.install()
    try:
        subprocess.call(['python3', 'infra'])
    except Exception as e:
        subprocess.call(['python', 'infra'])