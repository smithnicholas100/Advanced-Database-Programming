from pymongo import MongoClient
import urllib
import pprint
from secrets import secrets
from datetime import date
from mysql.connector import connect, Error

uri = "mongodb+srv://nicholassmith2:" + urllib.parse.quote(secrets.get('MONGO_PW')) + "@cluster0.9akr6wn.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# for db in client.list_database_names():
#     print(db)

db = client.employee_appreciation

db.bonuses.insert_many([
    { 'yearsOfService': 1, 'bonusAmount': 50 },
    { 'yearsOfService': 5, 'bonusAmount': 500 },
    { 'yearsOfService': 10, 'bonusAmount': 1000 },
    { 'yearsOfService': 15, 'bonusAmount': 1500 },
    { 'yearsOfService': 20, 'bonusAmount': 3000 },
    { 'yearsOfService': 25, 'bonusAmount': 4000 },
    { 'yearsOfService': 30, 'bonusAmount': 5000 }
])

secret_key = secrets.get('SECRET_KEY')
db_user = secrets.get('DATABASE_USER')
db_pass = secrets.get('DATABASE_PASSWORD')
db_port = secrets.get('DATABASE_PORT')

try:
    cnx = connect(
        host='localhost',
        user=db_user,
        password=db_pass, 
        database='employees'
    ) 
    
except Error as e:
    print(e)

query = """SELECT concat(first_name, " ", e.last_name) as 'Employee',
CASE
    WHEN DATE_ADD(e.hire_date, INTERVAL 25 YEAR) BETWEEN DATE_SUB(curdate(), INTERVAL 1 WEEK) AND curdate() THEN 25
    WHEN DATE_ADD(e.hire_date, INTERVAL 30 YEAR) BETWEEN DATE_SUB(curdate(), INTERVAL 1 WEEK) AND curdate() THEN 30
END as yearsOfService
FROM employees e
	JOIN 		
    titles t
    ON e.emp_no = t.emp_no
WHERE 
	t.to_date = "9999-01-01" AND 
    (DATE_ADD(e.hire_date, INTERVAL 25 YEAR) BETWEEN DATE_SUB(curdate(), INTERVAL 1 WEEK) AND curdate()
		OR DATE_ADD(e.hire_date, INTERVAL 30 YEAR) BETWEEN DATE_SUB(curdate(), INTERVAL 1 WEEK) AND curdate())
ORDER BY 'Employee'"""

cursor = cnx.cursor()
print(cnx)
print() 

from prettytable import PrettyTable
table = PrettyTable()
table.field_names = ['Employee', 'Bonus Amount']

cursor.execute(query)
result = cursor.fetchall()
print(len(result), "employee 25-year and 30-year appreciations this week")

for row in result: 
    # doc = db.bonuses.find_one({'yearsOfService': row[1]}).get('bonusAmount')
    # table.add_row([row[0], "$%s" %doc.get('bonusAmount')])
    table.add_row([row[0], "$%s" %db.bonuses.find_one({'yearsOfService': row[1]}).get('bonusAmount')])
    # print(row[0] + "\t%s" %row[1] + "\t%s" %hire_date + "\t$%s" %doc.get('bonusAmount'))

print(table)

cursor.close()
cnx.close()
