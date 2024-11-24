import re
import pymysql
from flask import Flask, render_template, request, jsonify
import Emulation

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index3.html')

@app.route('/showd', methods=['POST'])
def showd():
    data = request.get_json()
    #print(data)

    #emu=Emulation.MongoEmu()

    db=Emulation.SQLDB()
    #user_input = data.get('input')

    #result=emu.run(user_input)
    # Process the input data using your Python function

    result = db.showt()
    #print(result)
    mongo_output = "MongoDB Command:   show collections"
    rows = []
    rows.append(mongo_output)
    counter=0
    for row in result:
        for k,v in row.items():
            if counter==0:
                rows.append(k+":")
            rows.append(v)
            counter+=1
    result = rows
    #result = your_python_function(user_input)
    #result="Success1"
    #print(result)
    #mongo_output="MongoDB Command:   show collections"
    #result=[mongo_output,rows]
    # Return the result as a JSON object
    return jsonify(result=result)

@app.route('/showTable', methods=['POST'])
def showTable():
    data = request.get_json()
    #print(data)

    #emu=Emulation.MongoEmu()

    db=Emulation.SQLDB()
    user_input = data.get('input')

    #result=emu.run(user_input)
    # Process the input data using your Python function
    if '(' in user_input:
        match = re.match(r'(\w+)\((.*)\)', user_input)
        match=match.groups()
        cname=match[0]
        condition=match[1].split(",")
        #print(condition)
        cond_query="{"
        counter=1
        for cond in condition:
            if counter==len(condition):
                cond_query += cond+":1"
            else:
                cond_query += cond + ":1, "
            counter+=1
        mongo_query ="{}, "+cond_query+", _id:0}"
        cond_query+="}"

        cond_query="{}, "+cond_query
        command=f'db.{cname}.find({cond_query})'
        mongo_command = f'MongoDB Command:  db.{cname}.find({mongo_query})'
    else:
        command=f'db.{user_input}.find()'
        mongo_command = 'MongoDB Command:  ' + command
    print(command)
    result = db.run(command)
    rows = []
    rows.append(mongo_command)
    for row in result:
        row_dict = {}
        for col in row:
            row_dict[col] = row[col]
        rows.append(row_dict)

    result = rows
    #result = your_python_function(user_input)
    #result="Success1"
    # Return the result as a JSON object
    return jsonify(result=result)

@app.route('/insert', methods=['POST'])
def insert():
    data = request.get_json()
    #print(data)

    #emu=Emulation.MongoEmu()

    db=Emulation.SQLDB()
    user_input = data.get('input')
    document=''
    match = re.match(r"(\w+)\((.*)\)", user_input)
    # print(match)
    line = match.groups()
    table_name = line[0]
    param = line[1]
    count = param.split(",")
    counter=0
    for condition in count:
        counter+=1
        match = re.match(r"(.*)=(.*)", condition)
        line = match.groups()
        if counter<len(count):
            format=f'"{line[0].strip()}" : "{line[1].strip()}",'
        else:
            format = f'"{line[0].strip()}" : "{line[1].strip()}"'
        document=document+format
    document='{'+document+'}'
    command = f'db.{table_name}.insertOne({document})'
    result=db.run(command)
    result = ['success']
    mongo_command = f'MongoDB Command: db.{table_name}.insertOne({document})'
    mongo_command = mongo_command.replace("\"", "'")
    result.append(mongo_command)
    return jsonify(result=result)

@app.route('/update', methods=['POST'])
def update():
    data = request.get_json()
    #print(data)

    #emu=Emulation.MongoEmu()

    db=Emulation.SQLDB()
    user_input = data.get('input')
    document=''
    match = re.match(r"(\w+)\((.*)\)\((.*)\)", user_input)
    # print(match)
    line = match.groups()
    table_name = line[0]
    condition = line[1]

    match = re.match(r"(.*)==(.*)", condition)
    condition=match.groups()
    print("condition[0]="+condition[0])
    Cformat = f"{condition[0]} : '{condition[1]}'"
    param = line[2]
    match = re.match(r"(.*)=(.*)", param)
    param=match.groups()

    Pformat =  f"'{param[0]}' : '{param[1]}'"
    document='{'+Cformat+'}, {$set: {'+Pformat+'}}'
    command = f'db.{table_name}.updateOne({document})'
    #result=emu.run(user_input)
    #print(document)
    # Process the input data using your Python function
    #db.updateOne(table_name, document)
    result=db.run(command)
    # print(document)
    result = ['success']
    mongo_command = f'MongoDB Command: db.{table_name}.updateOne({document})'
    mongo_command = mongo_command.replace("\"", "'")
    result.append(mongo_command)

    #result = your_python_function(user_input)
    # Return the result as a JSON object
    return jsonify(result=result)

@app.route('/find', methods=['POST'])
def find():
    data = request.get_json()
    #print(data)

    #emu=Emulation.MongoEmu()
    #test2(name=andy)
    db=Emulation.SQLDB()
    user_input = data.get('input')
    document = ""
    #test2(name=andy,age=3)(name,age)
    match = re.match(r"(\w+)\((.*)\)", user_input)
    #print(match)
    line=match.groups()
    #print(line)
    table_name=line[0]
    param=line[1]

    param = param.split(")(")
    print(len(param))
    if len(param)==1:
        count = param[0].split(",")
        counter = 1
        for condition in count:
            match = re.match(r"(.*)=(.*)", condition)
            line = match.groups()
            v = line[1].strip()
            if isinstance(v, str):
                v = f'"{v}"'
            else:
                v = v
            if counter == len(count):
                document += f'{line[0].strip()}:{v}'
            else:
                document += f'{line[0].strip()}:{v},'
            counter += 1
        document = "{" + document + "}"
        command = f'db.{table_name}.find({document})'
        mongo_command = "MongoDB Command:    " + command
        mongo_command=mongo_command.replace('"','')
        result = db.run(command)
        # print(document)
        # 'db.new.find({name:"a"}, {age:1, name:1})'
        rows = [mongo_command]
        for row in result:
            row_dict = {}
            for col in row:
                row_dict[col] = row[col]
            rows.append(row_dict)
        result = rows
        # result = your_python_function(user_input)
        # Return the result as a JSON object
        return jsonify(result=result)

    count=param[0].split(",")
    counter=1
    for condition in count:
        match = re.match(r"(.*)=(.*)", condition)
        line = match.groups()
        v=line[1].strip()
        if isinstance(v,str):
            v=f'"{v}"'
        else:
            v=v
        if counter==len(count):
            document+=f'{line[0].strip()}:{v}'
        else:
            document += f'{line[0].strip()}:{v},'
        counter+=1
    document= "{"+document+"}"
    #print(param)
    proj=param[1].split(",")
    pro_command=""
    counter=1
    for projection in proj:
        if len(proj)==counter:
            pro_command+=projection+":1"
        else:
            pro_command += projection + ":1,"
        counter+=1
    pro_command="{"+pro_command+"}"
    #print(document)
    #print(pro_command)
    command=f'db.{table_name}.find({document},{pro_command})'
    #print(command)
    mongo_command="MongoDB Command:    "+command
    mongo_command = mongo_command.replace('"', '')
    result=db.run(command)
    #print(document)
    #'db.new.find({name:"a"}, {age:1, name:1})'
    rows = [mongo_command]
    for row in result:
        row_dict = {}
        for col in row:
            row_dict[col] = row[col]
        rows.append(row_dict)
    result = rows
    #result = your_python_function(user_input)
    # Return the result as a JSON object
    return jsonify(result=result)

@app.route('/join', methods=['POST'])
def join():
    data = request.get_json()
    #print(data)

    #(table_name1,table_name2)(attr1,attr2)(project_attr1,project_attr2)
    #test2(name=andy)
    db=Emulation.SQLDB()
    user_input = data.get('input')
    document = ""
    #result=[]
    #test2(name=andy,age=3)(name,age)
    match = re.match(r"\((.*)\)\((.*)\)", user_input)

    match=match.groups()
    #print("match1")
    #print(match[0])
    if ")(" not in match[0]:
        tables=match[0].split(",")
        name1=tables[0]
        name2=tables[1]
        attrs=match[1].split(",")
        attr1=attrs[0]
        attr2 = attrs[0]
        command = f'db.{name1}.aggregate'+'([{$lookup:{'+f'from: {name2} , localField: {attr1}, foreignField: {attr2}, as: {attr1}'+'}}])'
        output=db.run(command)
    else:
        match0 = match[0].split(")(")
        tables=match0[0].split(",")
        name1 = tables[0]
        name2 = tables[1]

        attrs = match0[1].split(",")
        attr1 = attrs[0]
        attr2 = attrs[0]
        projs=match[1].split(",")
        proj1=projs[0]
        proj2=projs[1]
        command = f'db.{name1}.aggregate' + '([{$lookup:{' + f'from: {name2} , localField: {attr1}, foreignField: {attr2}, as: {attr1}' + '}}, {$project:{'+f'{proj1}:1,{proj2}:1'+'}}])'
        print(command)
        output = db.run(command)
    mongo_command = "MongoDB Command:   " + command
    rows = [mongo_command]
    #print(output)
    for row in output:
        row_dict = {}
        for col in row:
            row_dict[col] = row[col]
        rows.append(row_dict)

    result = rows

    return jsonify(result=result)

@app.route('/aggregate', methods=['POST'])
def aggregate():
    data = request.get_json()
    #print(data)

    #table_name1(attr1)(max,age)
    #'db.test2.aggregate( $group:{_id: "name", Maxage: {"$avg":"$age"}})'
    db=Emulation.SQLDB()
    user_input = data.get('input')
    document = ""
    #result=[]
    #test2(name=andy,age=3)(name,age)
    match = re.match(r"(\w+)\((.*)\)\((.*)\)", user_input)

    line=match.groups()
    #print("match1")
    #print(match[0])
    #line=match.groups()
    tablename=line[0]
    attr=line[1]
    cond=line[2].split(",")
    opera=cond[0]
    attr1=cond[1]
    alias=opera+attr1

    command= f'db.{tablename}.aggregate( $group:'+'{_id: "'+attr+f'", {alias}'+': {"$'+opera+'":"$'+attr1+'"}})'
    mongo_command='MongoDB Command:    '+command
    mongo_command=mongo_command.replace('"','')
    output=db.run(command)
    rows = [mongo_command]
    # print(output)
    for row in output:
        row_dict = {}
        for col in row:
            row_dict[col] = row[col]
        rows.append(row_dict)

    result = rows
    return jsonify(result=result)

@app.route('/create', methods=['POST'])
def create():
    data = request.get_json()
    #print(data)

    #emu=Emulation.MongoEmu()

    db=Emulation.SQLDB()
    user_input = data.get('input')
    command=f'db.createCollection("{user_input}")'
    #result=emu.run(user_input)
    # Process the input data using your Python function
    db.run(command)
    # result = your_python_function(user_input)
    output = "You just created table: " + user_input
    mongo_command = f"MongoDB Command: db.createCollection('{user_input}')"
    result = [output, mongo_command]
    return jsonify(result=result)

@app.route('/no_input', methods=['POST'])
def no_input():
    result="Please select a function!!!"
    # Return the result as a JSON object
    return jsonify(result=result)

if __name__ == '__main__':
    app.run(debug=True)