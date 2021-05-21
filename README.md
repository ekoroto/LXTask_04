# Task 4
## Description
This program gets 3 input arguments: 
* path to file `students.json`
* path to file `rooms.json` 
* format of export: `xml` or `json`

Creates MySQL DB, insert data from `students.json` and `rooms.json` into DB tables `students` and `rooms`, queries the following data:
* list of rooms with number of students
* top 5 rooms with the smallest average age of students
* top 5 rooms with the biggest difference in students age
* list of rooms with students of different sexes

and writes it to the corresponding format of export in `output_data` folder of this project. 

DB settings connection can be set in the program:
```
module db_handler.py -> DBHandler().creat_connection()
```
or can be set in your MySQL connection settings:
```
host='localhost',
user='root',
password='passwroot',
database='LXTask_04'
``` 
Example of input:
```python3 main.py input_data/students.json input_data/rooms.json json```