# https://sqlparse.readthedocs.io/en/latest/intro/
# https://github.com/andialbrecht/sqlparse

import sqlparse

parsed = sqlparse.parse('create database dsci')
parsed[0].tokens
parsed[0]._pprint_tree()
# note mongodb does not create database right away



parsed = sqlparse.parse('create table person(ssn int, name char(20), age int)')
parsed[0].tokens
parsed[0]._pprint_tree()

# convert it into: db.create

parsed = sqlparse.parse('select * from person where age >= 20')
parsed[0].tokens

parsed[0]._pprint_tree()

# convert it to person.find({"age": {$gte: 20}})

parsed = sqlparse.parse('insert into R(a, b, c) values(1, "john", 3)')
parsed[0].tokens

parsed[0]._pprint_tree()

