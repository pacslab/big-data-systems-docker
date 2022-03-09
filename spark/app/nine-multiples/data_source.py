"""
    This script generates random integers and forwards them through a local connection in port 9999. 
    The data stream is meant to be read by a spark app for processing.
    Both the data source app and spark app are designed to be run in Docker containers.

    Made for: EECS 4415 - Big Data Systems (Department of Electrical Engineering and Computer Science, York University)
    Author: Changyuan Lin
"""


import sys
import socket
import random
import time

TCP_IP = "0.0.0.0"
TCP_PORT = 9999
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting sending data.")
while True:
    try:
        number = random.randint(1, 1000000000)
        data = f"{number}\n".encode()
        conn.send(data)
        print(number)
        time.sleep(1)
    except KeyboardInterrupt:
        s.shutdown(socket.SHUT_RD)
