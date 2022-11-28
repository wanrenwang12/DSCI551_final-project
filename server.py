#!/usr/bin/env python
# coding: utf-8

# In[3]:


import requests
import json
import csv

namenode_url = "https://dsci551-77144-default-rtdb.firebaseio.com/namenode"
datanode_url = "https://dsci551-77144-default-rtdb.firebaseio.com/datanode"


def cd(path):
    global namenode_url
    if "https://dsci551-77144-default-rtdb.firebaseio.com" in path:
        namenode_url = path
    elif path == "..":
        temp = namenode_url.split("/")
        namenode_url = "/".join(temp[:-1])
    else:
        namenode_url = namenode_url + '/' + path
    return namenode_url


def mkdir(path):
    data = path.split("/")
    url = namenode_url + "/".join(data[:-1]) + ".json"
    temp_dict = {data[-1]: ""}
    data = json.dumps(temp_dict)
    response = requests.patch(url, data)
    response.text
    response.json()
    return "Success."


def ls(path):
    url = namenode_url + path + ".json"
    response = requests.get(url)
    data = json.loads(json.dumps(response.json(), indent=4))
    res = []
    if not data:
        return "This directory is empty."
    for key, value in data.items():
        res.append(key)
    return ' '.join(res)


def put(file, path, k):
    k = int(k)
    hashMap = {}
    file_name = file.split(".")
    count = 0
    with open(file) as f:
        reader = csv.reader(f)
        for rows in reader:
            titles = rows
            n = len(titles)
            # print(n)
            break
        num_lines_total = 0
        for rows in reader:
            num_lines_total += 1
            tempHashMap = {}
            for i in range(n):
                try:
                    float(rows[i])
                except:
                    isFloat = False
                else:
                    isFloat = True
                try:
                    int(rows[i])
                except:
                    isDigit = False
                else:
                    isDigit = True
                if isFloat:
                    rows[i] = float(rows[i])
                elif isDigit:
                    rows[i] = int(rows[i])
                tempHashMap[titles[i]] = rows[i]
            hashMap[count] = tempHashMap
            count += 1

    num_lines = int(int(num_lines_total) / k)
    data = []
    split_start = 0
    split_end = num_lines
    num = k
    while k > 0:
        data.append({k: hashMap[k] for k in list(hashMap.keys())[split_start:split_end]})
        split_start += num_lines
        split_end += num_lines
        k -= 1
    # print(len(data), data[0])

    for i in range(1, num + 1):
        data_split = json.dumps(data[i - 1])
        url = datanode_url + "/" + file_name[0] + "_" + str(i) + ".json"
        response = requests.put(url, data_split)
        response.text
        response.json()

    url = namenode_url + path + "/" + file_name[0] + ".json"
    # print(url)
    temp_dict = {"type": file_name[-1]}
    for i in range(1, num + 1):
        temp_dict[i] = file_name[0] + "_" + str(i) + ".json"
    data_name = json.dumps(temp_dict)
    response = requests.put(url, data_name)
    response.text
    response.json()
    return "Success."


def cat(path):
    path_split = path.split(".")
    url = namenode_url + ".".join(path_split[:-1]) + ".json"
    response = requests.get(url)
    data = json.loads(json.dumps(response.json(), indent=4))
    # print(data)

    if not data:
        return "This file is empty."

    data_location = []
    for key, value in data.items():
        if ".json" in value:
            data_location.append(value)
    # print(data_location)

    data_total = []
    for location in data_location:
        url = datanode_url + "/" + location
        # print(url)
        response = requests.get(url)
        data = json.loads(json.dumps(response.json(), indent=4))
        data = list(filter(None, data))
        # print(data)
        data_total += data

    # print(data_total)

    return str(data_total).lstrip("[").rstrip("]")


def rm(path):
    path_split = path.split(".")
    url = namenode_url + ".".join(path_split[:-1]) + ".json"
    # print(url)
    response = requests.get(url)
    data = json.loads(json.dumps(response.json(), indent=4))
    if not data:
        return "This file doesn't exist."

    requests.delete(url)

    url = "/".join(url.split("/")[:-1]) + ".json"
    # print(url)
    # temp_dict = {data[-1]: ""}
    # dir_data = json.dumps(temp_dict)

    response = requests.get(url)
    dir_data = json.loads(json.dumps(response.json(), indent=4))

    if not dir_data:
        response = requests.put(url, '""')
        response.text
        response.json()
    # print(data)

    data_location = []
    for key, value in data.items():
        if ".json" in value:
            data_location.append(value)
    # print(data_location)

    for location in data_location:
        url = datanode_url + "/" + location
        # print(url)
        requests.delete(url)
    return "Success."


def getPartitionLocations(path):
    path_split = path.split(".")
    url = namenode_url + ".".join(path_split[:-1]) + ".json"
    response = requests.get(url)
    data = json.loads(json.dumps(response.json(), indent=4))
    # print(data)

    if not data:
        return "This file doesn't exist."

    data_location = []
    for key, value in data.items():
        if ".json" in value:
            data_location.append(value)

    # print(data_location)
    return " ".join(data_location)


def readPartition(path, num_partition):
    path_split = path.split(".")
    url = namenode_url + ".".join(path_split[:-1]) + ".json"
    response = requests.get(url)
    data = json.loads(json.dumps(response.json(), indent=4))
    # print(data)

    if not data:
        return "This file doesn't exist."

    data_location = []
    for key, value in data.items():
        if ".json" in value:
            data_location.append(value)
    # print(data_location)

    location = data_location[int(num_partition) - 1]
    url = datanode_url + "/" + location
    # print(url)
    response = requests.get(url)
    data = json.loads(json.dumps(response.json(), indent=4))
    data = list(filter(None, data))
    # print(data)

    return str(data).lstrip("[").rstrip("]")


def readPartition_data(path, num_partition):
    path_split = path.split(".")
    url = namenode_url + ".".join(path_split[:-1]) + ".json"
    response = requests.get(url)
    data = json.loads(json.dumps(response.json(), indent=4))
    # print(data)

    if not data:
        return "This file doesn't exist."

    data_location = []
    for key, value in data.items():
        if ".json" in value:
            data_location.append(value)
    # print(data_location)

    location = data_location[num_partition - 1]
    url = datanode_url + "/" + location
    # print(url)
    response = requests.get(url)
    data = json.loads(json.dumps(response.json(), indent=4))
    data = list(filter(None, data))
    # print(data)

    return data


def mapPartition(path: str, parameters: str) -> str:
    result = []
    hash_map = json.loads(parameters)
    # print(hash_map)
    partitions = getPartitionLocations(path)
    partitions = partitions.split(" ")
    n = len(partitions)
    for i in range(1, n + 1):
        data = readPartition_data(path, i)
        # print(data)
        for row in data:
            # print(row.items())
            flag = 0
            for key, value in hash_map.items():
                if len(value) == 1:
                    if row[key] != value[0]:
                        flag = 1
                        break
                else:
                    if row[key] <= value[0] or row[key] >= value[1]:
                        flag = 1
                        break
            if not flag:
                result.append(row)
    return str(result).lstrip("[").rstrip("]")


def mapPartition_data(path: str, parameters: str) -> list[dict]:
    result = []
    hash_map = json.loads(parameters)
    # print(hash_map)
    partitions = getPartitionLocations(path)
    partitions = partitions.split(" ")
    n = len(partitions)
    for i in range(1, n + 1):
        data = readPartition_data(path, i)
        # print(data)
        for row in data:
            # print(row.items())
            flag = 0
            for key, value in hash_map.items():
                if len(value) == 1:
                    if row[key] != value[0]:
                        flag = 1
                        break
                else:
                    if row[key] <= value[0] or row[key] >= value[1]:
                        flag = 1
                        break
            if not flag:
                result.append(row)
    return result


def reduce(map_data: list[dict], projects: list[str]):
    result = []
    for element in map_data:
        hash_map = {}
        for project in projects:
            hash_map[project] = element[project]
        result.append(hash_map)
    return str(result).lstrip("[").rstrip("]")


# In[ ]:


import socket

ip_port = ('127.0.0.1', 8080)
command_len = {}
command_len['cd'] = 2
command_len['mkdir'] = 2
command_len['ls'] = 2
command_len['cat'] = 2
command_len['rm'] = 2
command_len['put'] = 4
command_len['getPartitionLocations'] = 2
command_len['readPartition'] = 3


sk = socket.socket()            # create socket
sk.bind(ip_port)                # bind
sk.listen(5)                    # listen
print('The server has been started')
conn, address = sk.accept()     # wait for client
while True:     
    client_data = conn.recv(1024).decode()      # receive data
    if client_data == "exit":       # if exit, the exit
        exit("ByeBye")
    orders = client_data.split()
    if orders[0] not in command_len or command_len[orders[0]] != len(orders): # illegal command
        conn.sendall('The command you input has error'.encode())
        continue
    res = None
    if orders[0] == 'cd':
        res = cd(orders[1])
    elif orders[0] == 'mkdir':
        res = mkdir(orders[1])
    elif orders[0] == 'ls':
        res = ls(orders[1])
    elif orders[0] == 'cat':
        res = cat(orders[1])
    elif orders[0] == 'rm':
        res = rm(orders[1])
    elif orders[0] == 'put':
        res = put(orders[1], orders[2], orders[3])
    elif orders[0] == 'getPartitionLocations':
        res = getPartitionLocations(orders[1])
    else:
        res = readPartition(orders[1], orders[2])

    conn.sendall(res.encode())    
conn.close()   


# In[ ]:




