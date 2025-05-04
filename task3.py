from mysql.connector import connect, Error
from secrets import secrets

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

# make company emails
names= []
hash = {}
records_to_update = []

cursor.execute("SELECT first_name, last_name, emp_no FROM employees.employees")
for row in cursor:
    temp = row[0][0] + row[1]
    temp = temp.lower()
    names.append(temp)
    if names[-1] not in hash:
        hash[names[-1]] = 1
        names[-1] = names[-1] + '@company.net'
    else:
        count = hash[names[-1]]
        hash[names[-1]] += 1

        # Append frequency count 
        names[-1] = names[-1] + str(count) + '@company.net'
    record = (names[-1], row[2])
    records_to_update.append(record)

print(len(records_to_update), 'company emails will be added.')

# update company emaails to database
try:
    sql_update_query = """Update employees.employees set company_email = %s where emp_no = %s"""

    # multiple records to be updated in tuple format
    cursor.executemany(sql_update_query, records_to_update)
    cnx.commit()

    print(cursor.rowcount, "Records updated successfully")

except Error as error:
    cnx.rollback()
    print("Failed to update records to database: {}".format(error))

# make the personal emails for senior people
names= []
hash = {}
records_to_update = []

query_for_senior_emails = """SELECT first_name, last_name, e.emp_no 
FROM employees e JOIN titles t ON e.emp_no = t.emp_no 
WHERE title LIKE '%Senior%'"""
cursor.execute(query_for_senior_emails)
for row in cursor:
    temp = row[0][0] + row[1]
    temp = temp.lower()
    names.append(temp)
    if names[-1] not in hash:
        hash[names[-1]] = 1
        names[-1] = names[-1] + '@personal.com'
    else:
        count = hash[names[-1]]
        hash[names[-1]] += 1

        # Append frequency count 
        names[-1] = names[-1] + str(count) + '@personal.com'
    record = (names[-1], row[2])
    records_to_update.append(record)

print(len(records_to_update), 'personal emails will be added.')

# update the personal emails to database
try:
    sql_update_query = """Update employees.employees set personal_email = %s where emp_no = %s"""

    # multiple records to be updated in tuple format
    cursor.executemany(sql_update_query, records_to_update)
    cnx.commit()

    print(cursor.rowcount, "Records updated successfully")

except Error as error:
    cnx.rollback()
    print("Failed to update records to database: {}".format(error))

hash = {}
records_to_update = []

query_for_phone_numbers = """SELECT emp_no FROM employees.employees"""
cursor.execute(query_for_phone_numbers)
for row in cursor:
    number = "801-6"
    if len(str(row[0])) == 5:
        number = number + "0" + str(row[0])[0] + "-" + str(row[0])[-4:] 
    elif len(str(row[0])) == 6:
        number = number + str(row[0])[0:2] + "-" + str(row[0])[-4:]  
    record = (number, row[0])
    records_to_update.append(record)

print(len(records_to_update), 'phone numbers will be added')

try:
    sql_update_query = """Update employees.employees set company_phone = %s where emp_no = %s"""

    # multiple records to be updated in tuple format
    cursor.executemany(sql_update_query, records_to_update)
    cnx.commit()

    print(cursor.rowcount, "Records updated successfully")

except Error as error:
    cnx.rollback()
    print("Failed to update records to database: {}".format(error))

cursor.close()
cnx.close()

    




