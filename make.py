import os

#To Run: python3 make.py
if __name__ =='__main__':
    os.system('docker build -t asg3img .')
    os.system('docker network create --subnet=10.10.0.0/16 asg3net')
    os.system('start cmd /C docker run --rm -p 8082:8090 --net=asg3net --ip=10.10.0.2 --name=alice -e=SOCKET_ADDRESS=10.10.0.2:8090 -e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090 asg3img')
    os.system('start cmd /C docker run --rm -p 8083:8090 --net=asg3net --ip=10.10.0.3 --name=bob -e=SOCKET_ADDRESS=10.10.0.3:8090 -e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090 asg3img')
    os.system('start cmd /C docker run --rm -p 8084:8090 --net=asg3net --ip=10.10.0.4 --name=carol -e=SOCKET_ADDRESS=10.10.0.4:8090 -e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090 asg3img')