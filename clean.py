import os

#To Run: python3 clean.py
if __name__ =='__main__':
    os.system('docker stop alice')
    os.system('docker stop bob')
    os.system('docker stop carol')
    os.system('docker network rm asg3net')
    os.system('docker image rm asg3img')