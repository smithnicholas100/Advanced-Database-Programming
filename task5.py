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

employed = []

# Retrieve the list of employees who are still employed.
cursor.execute("SELECT emp_no FROM employees.employees WHERE employment_status = 'E'")
for row in cursor:
    employed.append(row[0])

format_strings = ','.join(['%s'] * len(employed))
employed_titles = {}
current_salaries = {}
new_salaries = []

# retrieving current titles of employees still employed
cursor.execute("SELECT emp_no, title FROM employees.titles WHERE to_date = '9999-01-01' AND emp_no IN (%s)" % format_strings, tuple(employed))
for row in cursor: 
    employed_titles[row[0]] = row[1]

# retrieving current salaries of employees still employed
cursor.execute("SELECT emp_no, salary FROM employees.salaries WHERE to_date = '9999-01-01' AND emp_no IN (%s)" % format_strings, tuple(employed))
for row in cursor: 
    current_salaries[row[0]] = row[1]

# building a list of new salaries 

    # Assistant Engineer: 5% raise
    # Engineer: 7.5% raise
    # Manager: 10% raise
    # Senior Engineer: 7% raise
    # Senior Staff: 6.5% raise
    # Staff: 5% raise
    # Technique Leader: 8% raise
    
today = str(date.today())

# build list for updating old salary to_dates to today's date
records_to_update = []
for emp_no in employed:  
    record = (today, emp_no)
    records_to_update.append(record)

for emp_no in employed: 
    if employed_titles.get(emp_no) == "Assistant Engineer": 
        record = (emp_no, int(current_salaries.get(emp_no) * 1.05), today, '9999-01-01')
        new_salaries.append(record)
    elif employed_titles.get(emp_no) == "Engineer": 
        record = (emp_no, int(current_salaries.get(emp_no) * 1.075), today, '9999-01-01')
        new_salaries.append(record)
    elif employed_titles.get(emp_no) == "Manager": 
        record = (emp_no, int(current_salaries.get(emp_no) * 1.1), today, '9999-01-01')
        new_salaries.append(record)
    elif employed_titles.get(emp_no) == "Senior Engineer": 
        record = (emp_no, int(current_salaries.get(emp_no) * 1.07), today, '9999-01-01')
        new_salaries.append(record)
    elif employed_titles.get(emp_no) == "Senior Staff": 
        record = (emp_no, int(current_salaries.get(emp_no) * 1.065), today, '9999-01-01')
        new_salaries.append(record)
    elif employed_titles.get(emp_no) == "Staff": 
        record = (emp_no, int(current_salaries.get(emp_no) * 1.05), today, '9999-01-01')
        new_salaries.append(record)
    elif employed_titles.get(emp_no) == "Technique Leader": 
        record = (emp_no, int(current_salaries.get(emp_no) * 1.08), today, '9999-01-01')
        new_salaries.append(record)   

print(len(new_salaries), "rows should be inserted")

# UPDATE old salaries to_date into the salary table and INSERT new salaries
try:
    
    sql_update_old_salaries = """UPDATE employees.salaries SET to_date = %s WHERE emp_no = %s AND to_date = '9999-01-01'"""
    sql_insert_new_salaries = """INSERT INTO employees.salaries (emp_no, salary, from_date, to_date) VALUES (%s, %s, %s, %s)"""
    
    # multiple records to be updated in tuple format
    cursor.executemany(sql_update_old_salaries, records_to_update)
    print(cursor.rowcount, "rows updated")
    cursor.executemany(sql_insert_new_salaries, new_salaries)
    print(cursor.rowcount, "rows inserted")
    
    cnx.commit()

except Error as error:
    cnx.rollback()
    print("Failed to insert records to database: {}".format(error))
    
cursor.close()
cnx.close()