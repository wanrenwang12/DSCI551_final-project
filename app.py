import json

import streamlit as st
import commands_v7 as cmd
import pandas as pd
import base64

# 设置网页标题
st.title('Search and Analytics for Vehicles')


def image_local(image_file):
    with open(image_file, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    st.markdown(
        f"""
    <style>
    .stApp {{
        background-image: url(data:image/{"png"};base64,{encoded_string.decode()});
        background-size: cover
    }}
    </style>
    """,
        unsafe_allow_html=True
    )


image_local('bg2.jpg')

nv = cmd.getPartitionLocations('/new_vehicles.csv')
nv = nv.split(" ")
nn = len(nv)
# print(nv)
nvMakeSet = set()
nvModelSet = set()
nvEngSet = set()
make2Model = dict()
inputData = dict()

uv = cmd.getPartitionLocations('/used_vehicles.csv')
uv = uv.split(" ")
un = len(uv)
# print(nv)
uvMakeSet = set()
uvModelSet = set()
uvEngSet = set()
uvMake2Model = dict()
inputData = dict()

for i in range(1, nn + 1):
    data = cmd.readPartition_data('/new_vehicles.csv', i)
    for row in data:
        if row['Make'] not in make2Model:
            make2Model[row['Make']] = [row['Model']]
        elif row['Model'] not in make2Model[row['Make']]:
            make2Model[row['Make']].append(row['Model'])
        nvMakeSet.add(row['Make'])
        nvModelSet.add(str(row['Model']))
        nvEngSet.add(int(row['Engine Cylinders']))

for i in range(1, un + 1):
    data = cmd.readPartition_data('/used_vehicles.csv', i)
    for row in data:
        if row['Make'] not in uvMake2Model:
            uvMake2Model[row['Make']] = [row['Model']]
        elif row['Model'] not in uvMake2Model[row['Make']]:
            uvMake2Model[row['Make']].append(row['Model'])
        uvMakeSet.add(row['Make'])
        uvModelSet.add(str(row['Model']))
        uvEngSet.add(int(row['Engine Cylinders']))

nvMakeSet = list(nvMakeSet)
nvMakeSet.sort()
nvModelSet = list(nvModelSet)
nvModelSet.sort()
nvEngSet = list(nvEngSet)
nvEngSet.sort()

uvMakeSet = list(uvMakeSet)
uvMakeSet.sort()
uvModelSet = list(uvModelSet)
uvModelSet.sort()
uvEngSet = list(uvEngSet)
uvEngSet.sort()
# print(nvMakeSet)

# uv = cmd.getPartitionLocations('/used vehicles.csv')
# uv = uv.split(" ")
# nu = len(uv)


func = st.selectbox(
    'Function',
    ('Search', 'Analyze'))

if func == 'Search':
    data = st.selectbox(
        'Data',
        ('New vehicles', 'Used vehicles'))

    if data == 'New vehicles':
        make = st.multiselect('Make', nvMakeSet)
        # st.write(make)
        if not make:
            model = st.multiselect('Model', nvModelSet)
        else:
            temp = []
            for m in make:
                temp += make2Model[m]
            model = st.multiselect('Model', temp)

        yearStart = st.text_input('Year starts from')
        yearEnd = st.text_input('To')

        engineCylinders = st.multiselect('Engine Cylinders', nvEngSet)

        drivenWheels = st.multiselect('Driven Wheels', ['fwd', 'rwd', '4wd', 'other'])

        priceStart = st.slider('Price starts from', 0, 2065902)
        priceEnd = st.slider('To', 2065902, 0)
    elif data == 'Used vehicles':
        make = st.multiselect('Make', uvMakeSet)
        # st.write(make)
        if not make:
            model = st.multiselect('Model', uvModelSet)
        else:
            temp = []
            for m in make:
                temp += uvMake2Model[m]
            model = st.multiselect('Model', temp)

        yearStart = st.text_input('Year starts from')
        yearEnd = st.text_input('To')

        engineCylinders = st.multiselect('Engine Cylinders', uvEngSet)

        drivenWheels = st.multiselect('Driven Wheels', ['fwd', 'rwd', '4wd'])

        priceStart = st.slider('Price starts from', 0, 229500)
        priceEnd = st.slider('To', 229500, 0)

        condition = st.multiselect('Condition', ['new', 'like new', 'excellent', 'good', 'fair'])

        odometerStart = st.text_input('Odometer starts from')
        odometerEnd = st.text_input('ends at')
elif func == 'Analyze':
    data = st.selectbox(
        'Data',
        ('New vehicles', 'Used vehicles'))

    if data == 'New vehicles':
        make = st.multiselect('Make', nvMakeSet)
        # st.write(make)
        if not make:
            model = st.multiselect('Model', nvModelSet)
        else:
            temp = []
            for m in make:
                temp += make2Model[m]
            model = st.multiselect('Model', temp)

        yearStart = st.text_input('Year starts from')
        yearEnd = st.text_input('To')

        engineCylinders = st.multiselect('Engine Cylinders', nvEngSet)

        drivenWheels = st.multiselect('Driven Wheels', ['fwd', 'rwd', '4wd', 'other'])

        priceStart = st.slider('Price starts from', 0, 2065902)
        priceEnd = st.slider('To', 2065902, 0)

        group = st.selectbox('Group by', ['Make', 'Model', 'Year', 'Engine Cylinders', 'Driven Wheels', 'Price'])
        agg = st.selectbox('Aggregate function', ['count', 'sum', 'max', 'min', 'avg'])
        target = st.selectbox('The target of the aggregate function', ['*', 'Year', 'Engine Cylinders', 'Driven Wheels', 'Price'])
    elif data == 'Used vehicles':
        make = st.multiselect('Make', uvMakeSet)
        # st.write(make)
        if not make:
            model = st.multiselect('Model', uvModelSet)
        else:
            temp = []
            for m in make:
                temp += uvMake2Model[m]
            model = st.multiselect('Model', temp)

        yearStart = st.text_input('Year starts from')
        yearEnd = st.text_input('To')

        engineCylinders = st.multiselect('Engine Cylinders', uvEngSet)

        drivenWheels = st.multiselect('Driven Wheels', ['fwd', 'rwd', '4wd'])

        priceStart = st.slider('Price starts from', 0, 229500)
        priceEnd = st.slider('To', 229500, 0)

        condition = st.multiselect('Condition', ['new', 'like new', 'excellent', 'good', 'fair'])

        odometerStart = st.text_input('Odometer starts from')
        odometerEnd = st.text_input('ends at')

        group = st.selectbox('Group by', ['Make', 'Model', 'Year', 'Engine Cylinders', 'Driven Wheels', 'Price', 'Odometer', 'Condition'])
        agg = st.selectbox('Aggregate function', ['count', 'sum', 'max', 'min', 'avg'])
        target = st.selectbox('The target of the aggregate function', ['*', 'Year', 'Engine Cylinders', 'Driven Wheels', 'Price', 'Odometer'])

isClicked = st.button(func)

# st.write(cmd.mapPartition_analytics_data('/new_vehicles.csv', '{}', 'Make', 'count', '*'))

if isClicked:
    if data == 'New vehicles':
        path = '/new_vehicles.csv'
    elif data == 'Used vehicles':
        path = '/used_vehicles.csv'
        if condition:
            inputData["Condition"] = condition
        if odometerStart and odometerEnd:
            inputData["Odometer"] = [odometerStart, odometerEnd]
        elif odometerStart:
            inputData["Odometer"] = [odometerStart]
    if make:
        inputData["Make"] = make
    if model:
        inputData["Model"] = model
    if yearStart and yearEnd:
        inputData["Year"] = [int(yearStart), int(yearEnd)]
    elif yearStart:
        inputData["Year"] = [int(yearStart)]
    if engineCylinders:
        inputData["Engine Cylinders"] = engineCylinders
    if drivenWheels:
        inputData["Driven Wheels"] = drivenWheels
    if priceStart and priceEnd:
        inputData["Price"] = [priceStart, priceEnd]
    elif priceStart:
        inputData["Price"] = [priceStart]

    # st.write(drivenWheels, inputData.items())
    parameters = json.dumps(inputData)
    # st.write(parameters)
    if func == 'Search':
        d = cmd.mapPartition_search_data(path, parameters)
        df = pd.DataFrame(d)
        st.dataframe(df)
    elif func == 'Analyze':
        if not parameters:
            d = cmd.mapPartition_analytics_data(path, '{}', group, agg, target)
        else:
            d = cmd.mapPartition_analytics_data(path, parameters, group, agg, target)
        # st.write(d)
        keyList = []
        valueList = []
        for key, value in d.items():
            keyList.append(key)
            valueList.append(value)
        df = pd.DataFrame(valueList, keyList)
        # display = json.loads(d)
        # st.write(display)
        st.bar_chart(df)
