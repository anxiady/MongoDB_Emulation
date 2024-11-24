import decimal
import json
import re
import pymysql
import uuid

class SQLDB:
    def __init__(self):
        config = {'host': 'localhost',
                  'user': 'root',
                  'password': 'DSCI-351',
                  'database': 'emula',
                  'charset': 'utf8mb4',
                  'cursorclass': pymysql.cursors.DictCursor}
        self.db = pymysql.connect(**config)

    def run(self, input):
        match = re.match(r'(\w+)\.(.*)', input)
        if not match:
            print("Invalid format")
            return "Invalid format"
        input2=match.groups()
        yes_dot = re.match(r'(\w+)\.(.*)', input2[1])
        if not yes_dot:
            inputs=input2
        else:
            inputs = yes_dot.groups()
        #print(inputs)
        Collection_name = inputs[0]
        #print(inputs[1])
        match1=re.match(r'(\w+)\((.*)\)',inputs[1])
        #print(match1)
        if not match1:
            print("Invalid command")
            return "Invalid command"
        inputs1=match1.groups()
        param=inputs1[1].strip('"')
        param = param.strip("'")                    #whatever in ()
        command=str(inputs1[0])
        if command=="createCollection":
            db.createCollection(param)
        elif command=="insertOne":
            return db.insertOne(Collection_name,param)
        elif command=="find":
            #db.person.find({age: {$gt:20}}, {age:1, name:1})
            # db.find("test1", {"name": "Min", "age": 21}, ["age"])
            if param.strip()=="":                                           #consider db.table.find()
                return db.find(Collection_name, {}, [])
            param=param.split(",",1)
            if len(param)==1:                   #db.person.find({age: 3})
                param=param[0].replace("{","").replace("}","").split(":")
                document = f'"{param[0]}" : {param[1]}'
                document = "{" + document + "}"
                document = json.loads(document)
                return db.find(Collection_name, document)
            proj=param[1].replace("{","").replace("}","").split(",")
            projection=[]
            for cond in proj:
                cond=cond.replace(" ","").split(":")
                if cond[1]=="1":
                    projection.append(cond[0])
            if param[0].replace(" ","")=="{}":                              #consider db.table.find({},{name:1, age:1})
                document=json.loads(param[0])
                return db.find(Collection_name, document, projection)
            newparam=param[0].strip().strip("{").strip("}").split(":")
            document=f'"{newparam[0]}" : {newparam[1]}'
            document="{"+document+"}"
            document=json.loads(document)
            return db.find(Collection_name,document,projection)
        elif command=="updateOne":
            return db.updateOne(Collection_name,param)
        elif command=="aggregate":
            if "$lookup" in param:
                param = param.strip("[").strip("]")
                if (param == param.split("} ,{")[0]):
                    if (param == param.split("}, {")[0]):
                        param = param.strip("{").replace("}", "", 1).split("}, {")
                    else:
                        param = param.split("}, {")
                else:
                    param = param.split("} ,")
                if len(param) > 1:
                    param[0] = param[0].strip("{")
                    param[len(param) - 1] = param[len(param) - 1].replace("}", "", 1)

                counter = 0
                while counter < len(param):
                    param[counter] = param[counter].replace("$", '"$').replace(':', '":"').replace('}', '"}').replace(
                        ':"{', ':{"').replace(',', '","')
                    counter += 1
                pipeline = ""
                counter = 0
                while counter < len(param):
                    if counter == 0:
                        pipeline = param[counter]
                    else:

                        pipeline = pipeline + ',' + param[counter]
                    counter += 1

                pipeline = "{" + pipeline + "}"
                pipeline = pipeline.replace(' "', '"').replace('" ', '"')
                pipeline = json.loads(pipeline)
                name2 = pipeline["$lookup"]["from"].strip()
                local = pipeline["$lookup"]["localField"].strip()
                foreign = pipeline["$lookup"]["foreignField"].strip()
                if "$project" not in pipeline.keys():
                    proj = []
                else:
                    k = pipeline["$project"].keys()
                    proj = []
                    for key in k:
                        proj.append(key)
                pipeline1 = [pipeline]
                return db.join(Collection_name, name2, local, foreign, pipeline1, proj)
            else:
                param=param.replace('"','')
                param=param.replace("$", '"$').replace(':', '":"').replace('}', '"}').replace(':"{', ':{"').replace(',', '","').replace(':""',':"').replace('}"}','}}').replace(" ","").replace(':"{',':{')
                pipeline = "{" + param + "}"
                print(pipeline)
                pipeline=json.loads(pipeline)
                proj=pipeline["$group"]["_id"]
                pipeline1=[pipeline]
                return db.aggregate(Collection_name,pipeline1,[proj])
        else:
            print("Unknown command. \nOnly accept createCollection & insertOne")
            return "Unknown command. Only accept createCollection & insertOne"

    def createCollection(self, Cname):
        command=f'create table {Cname} (_id varchar(50) not null PRIMARY KEY)'
        try:
            with self.db.cursor() as cursor:
                cursor.execute(command)
            self.db.commit()
            success=1
            return success
        except Exception as e:
            return f"command failed: {e}"

    def drop(self,Cname):
        with self.db.cursor() as cursor:
            cursor.execute(f'drop table {Cname}')
        self.db.commit()

    def des(self,Cname):
        with self.db.cursor() as cursor:
            cursor.execute('describe'+Cname)
            result=cursor.fetchall()
        print(result)

    def insertOne(self,Cname, document):
        a=json.loads(document)
        id = str(uuid.uuid4())
        key_length=len(a.keys())
        val_length = len(a.values())
        insert_attr="(_id,"
        insert_val = '"' + id + '",'
        for k,v in a.items():
            if k not in self.fetch_attr(Cname):
                self.add_attr(Cname, k, v)
            insert_attr = insert_attr + k
            if k == list(a.keys())[key_length - 1]:
                insert_attr = insert_attr + ")"
            else:
                insert_attr = insert_attr + ","
            if isinstance(v, int):
                sval = str(v)
            elif isinstance(v,float):
                sval = str(v)
            else:
                sval = '"' + v + '"'
            insert_val = insert_val + sval
            if v == list(a.values())[val_length - 1]:
                insert_val = insert_val + ')'
            else:
                insert_val = insert_val + ','
        final_val = '(' + insert_val
        with self.db.cursor() as cursor:
            print(f'insert into {Cname}{insert_attr} values{final_val}')
            cursor.execute(f'insert into {Cname}{insert_attr} values{final_val}')
        self.db.commit()

    def updateOne(self, Cname, document):
        # db.employees.updateOne({city: 'Kansas City'}, {$set: {'state': 'Missouri'}})
        # UPDATE employees SET salary = 60000 WHERE id = 123;
        match = re.match(r'(.*)\,(.*)', document)
        if not match:
            print("Invalid format")
            return "Invalid format"
        input2 = match.groups()

        value=input2[1].replace("$set", "").strip()
        value=value.replace(":","",1)
        #takes care of the condition format
        #keys don't need quotes
        condition=input2[0].replace("{","").replace("}","")
        condition=condition.strip()
        condition=condition.split(':')
        #takes care of the format of the input
        value = value.replace("{","").replace("}","")
        value=value.strip()
        vals=value.split(":")
        vals[0]=vals[0].replace('"','')
        vals[0] = vals[0].replace("'","")
        if isinstance(vals[1], int) or isinstance(vals[1], float):
            vals[1]=vals[1]
            #print(vals[1])
        if isinstance(condition[1],int) or isinstance(condition[1],float):
            condition[1]=str(condition[1])
        with self.db.cursor() as cursor:
            command = f'UPDATE {Cname} SET {vals[0]} = {vals[1]} WHERE {condition[0]} = {condition[1]} Limit 1'
            # print(command)
            cursor.execute(command)
        self.db.commit()

    def describe(self, Cname):
        with self.db.cursor() as cursor:
            cursor.execute(f"describe {Cname}")
            result = cursor.fetchall()
        print(result)

    def showt(self):
        with self.db.cursor() as cursor:
            cursor.execute("show tables")
            result = cursor.fetchall()
        return result

    def dropt(self):
        with self.db.cursor() as cursor:
            cursor.execute("show tables")
            result = cursor.fetchall()
            for coll in result:
                a = coll['Tables_in_emula']
                cursor.execute(f"drop table {a}")
        self.db.commit()
        print("show tables:")

        self.showt()

    def select(self, Cname):
        with self.db.cursor() as cursor:
            cursor.execute(f"select * from {Cname}")
            result = cursor.fetchall()
        return result

    def fetch_attr(self, Cname):
        with self.db.cursor() as cursor:
            cursor.execute(f"describe {Cname}")
            result = cursor.fetchall()
        attr_list=[]
        for table in result:
                attr_list.append(table["Field"])
        return(attr_list)

    def add_attr(self, Cname, attr, value):
        with self.db.cursor() as cursor:
            if isinstance(value, int):
                cursor.execute(f"alter table {Cname} add column {attr} int")
            elif isinstance(value, str):
                cursor.execute(f"alter table {Cname} add column {attr} varchar(50)")
            elif isinstance(value, float):
                cursor.execute(f"alter table {Cname} add column {attr} float")
        self.db.commit()
    # updated code for find
    # db.collection.find() in mongoDB
    # parameter document = dictionary with key-value pairs/json
    def find(self, Cname, document, projection=[]):
        with self.db.cursor() as cursor:
            query = "SELECT"
            # add projection fields to the query
            if projection:
                for i in range(len(projection)):
                    if i == 0:
                        query += " "
                    else:
                        query += ", "
                    query += projection[i]
            # if projection is empty, select all fields
            else:
                query += " *"
            if document=={}:
                query+=" FROM " + Cname
                print(query)
                cursor.execute(query)
                # fetch result
                rows = cursor.fetchall()
                return rows
            query += " FROM " + Cname + " WHERE "
            # create an empty list for conditions
            conditions = []
            for key in document:
                condition = key + "='" + str(document[key]) + "'"
                conditions.append(condition)
            # append all conditions using AND
            query += " AND ".join(conditions)
            # execute full query string
            # full query = "SELECT * FROM " + Cname + " WHERE " + "AND ".join(conditions)
            cursor.execute(query)
            # fetch result
            rows = cursor.fetchall()
        return rows

    def join(self, Cname1, Cname2, local_field, foreign_field, pipeline, projection=[]):
        # construct join query and initialize where and select clauses
        join_query = f"JOIN {Cname2} ON {Cname1}.{local_field} = {Cname2}.{foreign_field}"
        where_query = ""
        select_query = ""

        # constructing select_query based on projection
        if projection:
            for i in range(len(projection)):
                if i == 0:
                    select_query += "SELECT "
                # if more than 1 field, add a comma
                else:
                    select_query += ", "
                # append all field names to select_query
                if projection[i] in self.fetch_attr(Cname1) and projection[i] in self.fetch_attr(Cname2):
                    select_query = select_query+ f'{Cname1}.'+ projection[i]
                else:
                    select_query += projection[i]

        # if projection is empty, select all fields
        else:
            select_query = "SELECT *"

        if pipeline:

            for stage in pipeline:
                # construct where clause if you see $match
                #print(stage)
                if "$match" in stage:
                    conditions = []
                    # iterate over key value pairs in $match, ("key":"value")
                    #print(stage["$match"])
                    for k, v in stage["$match"].items():
                        # append to conditions

                        conditions.append(f"{Cname1}.{k} = '{v}'")
                    # join all conditions
                    where_query = "WHERE " + " AND ".join(conditions)

                # construct join clause if you see $lookup
                if "$lookup" in stage:
                    join_stage = stage["$lookup"]
                    join_collection = join_stage["from"]  # get Cname2
                    join_local_field = join_stage["localField"]  # field name in Cname1
                    join_foreign_field = join_stage["foreignField"]  # field name in Cname 2
                    join_query = f"JOIN {join_collection} ON {Cname1}.{join_local_field} = {join_collection}.{join_foreign_field}"

        # Combine them into a full query
        full_query = f"{select_query} FROM {Cname1} {join_query} {where_query}"
        # Execute the SQL query and return the results
        with self.db.cursor() as cursor:
            print(full_query)  # print query to check
            cursor.execute(full_query)
            rows = cursor.fetchall()
        for row in rows:
            print(row)
        return rows

    def aggregate(self, Cname, pipeline, projection=[]):
        query = ""
        select_query = ""
        where_query = ""
        sort_query = ""
        group_by_query = []
        aggregation_expressions = {
            "$sum": "SUM",
            "$avg": "AVG",
            "$min": "MIN",
            "$max": "MAX",
            "$count": "COUNT"
        }

        # constructing select_query based on projection
        if projection:
            for i in range(len(projection)):
                if i == 0:
                    select_query = "SELECT "
                # if more than 1 field, add a comma
                else:
                    select_query += ", "
                # append all field names to select_query
                select_query += projection[i]
        # if projection is empty, select all fields
        else:
            select_query = "SELECT * "

        if pipeline:
            for stage in pipeline:
                # SQL equivalent = WHERE
                if "$match" in stage:
                    conditions = []
                    # iterate over key value pairs in $match, ("key":"value")
                    for k, v in stage["$match"].items():
                        # append to conditions
                        conditions.append(f"{k} = '{v}'")
                    # join all conditions
                    where_query = " WHERE " + " AND ".join(conditions)

                #  SQL equivalent = GROUP BY
                if "$group" in stage:
                    for k, v in stage["$group"].items():
                        # current key is "_id"
                        if k == "_id":
                            # check if value is a string, ex) $group {"_id":"product") here product is field name
                            if isinstance(v, str):
                                v = v.replace('$', '')
                                group_by_query = f" GROUP BY {v}"
                        else:
                            # if not _id but a dictionary, ex) "total": {"$sum":"$quantity"}
                            if isinstance(v, dict):
                                # loop over each key value pair
                                # agg_op= $sum, field= "quantity"
                                for agg_op, field in v.items():
                                    # check if it is a valid agg expression (exist in dictionary)
                                    if agg_op in aggregation_expressions:
                                        # if so, get the corresponding SQL query
                                        agg_func = aggregation_expressions[agg_op]
                                        # remove the $ sign
                                        col_name = field[1:]
                                        if not projection:
                                            # if projection is empty, only include aggregation functions
                                            select_query = f"SELECT {agg_func}({col_name}) AS {k}"
                                        else:
                                            # include both projection fields and aggregation functions
                                            select_query += f", {agg_func}({col_name}) AS {k}"
                                # query += f" FROM {Cname}"
                            elif isinstance(v, str):
                                select_query = f", {v} AS {k}"

                #  SQL equivalent = ORDER BY
                # if string and ascending, return output in alphabetical order, and vice versa
                # if int and ascending, return output in lowest to highest, and vice versa
                # for now assuming they only choose 1 field name/key
                if "$sort" in stage:
                    # iterate over key:value pairs in $sort
                    for key, direction in stage["$sort"].items():
                        # ex) $sort {"age": 1) then highest to lowest
                        if direction == 1:  # ascending
                            # extract the key = field name and append
                            sort_query += f" ORDER BY {key}"
                        # descending
                        elif direction == -1:
                            # append and add DESC keyword
                            sort_query += f" ORDER BY {key} DESC"

        query = f"{select_query} FROM {Cname} {where_query} {sort_query}"
        if "$group" in stage:
            query = f"{select_query} FROM {Cname} {where_query} {group_by_query} {sort_query}"

        try:
            with self.db.cursor() as cursor:

                print(query)
                cursor.execute(query)
                # print(where_query)
                # print(group_by_query)
                # print(sort_query)
                rows = cursor.fetchall()
                new_row = []
                for row in rows:
                    row_values = list(row.values())
                    for i in range(len(row_values)):
                        if isinstance(row_values[i], decimal.Decimal):
                            row_values[i] = float(row_values[i])
                    new_row.append(dict(zip(row.keys(), row_values)))
                return new_row
        except Exception as e:
            return f"command failed: {e}"


db=SQLDB()

#db.createCollection("test2")
#       Function Testing Area
#update_input='{"city": "Kansas City"}, {$set: {"state": "Missouri"}}'
#new_input='{ name: "a" }, { $set: { "age": 7 }}'
#db.updateOne("new", new_input)
#result=db.select("test2")

#input="db.test2.updateOne({name:'anXXdy'},{$set:{'age':'10'}})"
#input1='db.test2.insertOne({"name": "xxx", "age": 123})'

#result=db.select("test2")


#command='db.nw.updateOne({name: "andy"}, {$set: {"age": 10}})'
#command='db.nw.insertOne({"name": "andy", "age": 22})'
#command='db.createCollection("nw")'
#command='db.person.find({age:{$gt: 15}})'
#command='db.new.find()'
#command='db.new.find({name:"a"}, {age:1, name:1})'
#command='db.now.find()'
#db.run(command)
#db.dropt()
#print(db.select('now'))
#for v in db.run("db.new.find()"):
#    print(v)
#for v in db.run(command):
#    print(v)


#command='db.new.aggregate([{$match:{name:andy}}, {$lookup:{from: no , localField: name, foreignField: name, as: name}}, {$project:{name:1,age:1}}])'
#command='db.test2.aggregate([{$lookup:{from: no , localField: name, foreignField: name, as: name}}, {$project:{name:1,age:1}}])'
#command='db.test2.aggregate( $group:{_id: "name", Maxage: {"$avg":"$age"}})'
#db.drop("test1")

#for v in db.find("test2", {},[]):
    #print(v)
#db.describe("new")

