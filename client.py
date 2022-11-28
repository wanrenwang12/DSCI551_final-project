#!/usr/bin/env python
# coding: utf-8

# # EDFS System By JiaHua Guo and Dongdong Yang
# ## User's manual
#  - mkdir: create a directory in file system, e.g., mkdir /user/john
#  - ls:    listing content of a given directory, e.g., ls /user
#  - cat:   display content of a file, e.g., cat /user/john/hello.txt
#  - rm:    remove a file from the file system, e.g., rm /user/john/hello.txt
#  - put:   put: uploading a file to file system, e.g., put(cars.csv, /user/john, k = # partitions)
#  - getPartitionLocation: this method will return the locations of partitions of the file, 
#                          e.g., getPartitionLocations /test/cars.csv
#  - readPartition:this method will return the content of partition # of the specified file,
#                          e.g., readPartition /test/cars.csv 2
#  - ls: read all files and directories under the current path, e.g. ls /

# In[ ]:


import socket

ip_port = ('127.0.0.1', 8080)
s = socket.socket()     # create socket
s.connect(ip_port)      # connect to server

namenode_url = "https://dsci551-77144-default-rtdb.firebaseio.com/namenode"

while True:
    temp = 'root'
    if len(namenode_url) > 58:
        temp += namenode_url[58:]     
    inp = input(temp + '>').strip()
    if not inp:     # if input if null, continue
        continue
    s.sendall(inp.encode())

    if inp == "exit":   # if input == exit, then exit
        print("ByeBye！")
        break
    res = ''
    while True:
        server_reply = s.recv(1024).decode()
        res += server_reply
        if len(server_reply) < 1024:
            break
    if res[:5] == 'https':
        namenode_url = res
    else:
        if len(res) < 100:
            print(res)
        else:
            print(res[:100])

s.close()       # close


# In[ ]:




