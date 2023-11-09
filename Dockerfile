FROM ubuntu:latest
RUN apt-get update -y
RUN apt-get install python3-pip -y
RUN pip install flask
RUN pip install requests

ADD assignment3.py /

CMD [ "python3", "./assignment3.py", "-p 8090", "-h 0.0.0.0"]
