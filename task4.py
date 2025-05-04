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

with open('employees_cuts.csv') as file:
    content = file.readlines()

records_to_update = []
today = str(date.today())

for row in range(1, len(content)):
    record = (today, content[row][:-1])
    records_to_update.append(record)

print(len(records_to_update), 'records will be updated in 5 tables: employees, dept_emp, salaries, titles, dept_manager')

cursor = cnx.cursor()

try:
    update_termination_and_status = """UPDATE employees.employees SET employment_status = 'T', termination_date = %s WHERE emp_no = %s"""
    update_dept_emp = """UPDATE employees.dept_emp SET to_date = %s WHERE emp_no = %s AND to_date = '9999-01-01'"""
    update_salaries = """UPDATE employees.salaries SET to_date = %s WHERE emp_no = %s AND to_date = '9999-01-01'"""
    update_titles = """UPDATE employees.titles SET to_date = %s WHERE emp_no = %s AND to_date = '9999-01-01'"""
    update_dept_manager = """UPDATE employees.dept_manager SET to_date = %s WHERE emp_no = %s AND to_date = '9999-01-01'"""
    
    # multiple records to be updated in tuple format
    cursor.executemany(update_termination_and_status, records_to_update)
    print(cursor.rowcount, "rows updated in employees table")
    cursor.executemany(update_dept_emp, records_to_update)
    print(cursor.rowcount, "rows updated in dept_emp table")
    cursor.executemany(update_salaries, records_to_update)
    print(cursor.rowcount, "rows updated in salaries table")
    cursor.executemany(update_titles, records_to_update)
    print(cursor.rowcount, "rows updated titles table")
    cursor.executemany(update_dept_manager, records_to_update)
    print(cursor.rowcount, "rows updated in dept_manager table")
    cnx.commit()

    print("Records updated successfully")

except Error as error:
    cnx.rollback()
    print("Failed to update records to database: {}".format(error))

cursor.close()
cnx.close()
