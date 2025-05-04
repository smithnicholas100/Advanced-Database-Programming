from mysql.connector import connect, Error
from secrets import secrets
from datetime import date

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
    
print(cnx)    
cursor = cnx.cursor()

query = """SELECT 
    t.title, CONCAT('$',FORMAT(SUM(s.salary),0,'en_US'))
FROM
    employees.salaries s
        JOIN
    titles t ON s.emp_no = t.emp_no
WHERE
    s.to_date LIKE '9999-01-01'
        AND t.to_date = '9999-01-01'
GROUP BY t.title"""
print() 
cursor.execute(query)
print("JOB TITLE\t\tTOTAL SALARY EXPENSE")
print("---------\t\t--------------------")
for row in cursor:
    if row[0] == "Senior Engineer" or row[0] == "Engineer" or row[0] == "Senior Staff":
        print(row[0] + "\t\t%s" %row[1])
    elif row[0] == "Staff" or row[0] == "Manager":
        print(row[0] + "\t\t\t%s" %row[1])
    elif row[0] == "Assistant Engineer" or row[0] == "Technique Leader":
        print(row[0] + "\t%s" %row[1])

print()
print("""There are 3 main assumptions made in this salary expense projection:
      1) Report assumes no attrition for the next 12 months.
      2) Report assumes no change in titles for the next 12 months.
      3) Report assumes no additional raises for the next 12 months.\n""")

grand_total_query = """SELECT 
	CONCAT('$',FORMAT(SUM(s.salary),0,'en_US'))
FROM
    employees.salaries s
        JOIN
    titles t ON s.emp_no = t.emp_no
WHERE
    s.to_date LIKE '9999-01-01'
        AND t.to_date = '9999-01-01'"""
cursor.execute(grand_total_query)  
print("GRAND TOTAL SALARY EXPENSE")
print("--------------------------")
for row in cursor: 
    print(row[0])

print() 

cursor.close()
cnx.close()