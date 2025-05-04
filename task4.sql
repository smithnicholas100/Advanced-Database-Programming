ALTER TABLE employees
ADD termination_date date;

ALTER TABLE employees
ADD employment_status ENUM('E','T') DEFAULT 'E';
