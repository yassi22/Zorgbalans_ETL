from config import TABLES

def create_work_locations_table(cursor) -> None:
    """Create the work_locations table."""
    location_table = TABLES["work_location"]["table_name"]
    location_columns = TABLES["work_location"]["columns"]
    cursor.execute(f"""
    CREATE TABLE {location_table} (
        {location_columns['id']} SERIAL PRIMARY KEY,
        {location_columns['work_location']} VARCHAR(255) NOT NULL UNIQUE,
        {location_columns['address']} VARCHAR(255),
        {location_columns['postal_code']} VARCHAR(255)
    );
    """)


def create_work_location_employee_table(cursor) -> None:
    """Create the junction table for employees and work locations."""
    location_employee_table = TABLES["work_location_employee"]["table_name"]
    location_employee_columns = TABLES["work_location_employee"]["columns"]
    cursor.execute(f"""
    CREATE TABLE {location_employee_table} (
        {location_employee_columns['employee_id']} INTEGER REFERENCES {TABLES['employees']['table_name']}({TABLES['employees']['columns']['id']}),
        {location_employee_columns['work_location_id']} INTEGER REFERENCES {TABLES['work_location']['table_name']}({TABLES['work_location']['columns']['id']}),
        PRIMARY KEY ({location_employee_columns['employee_id']}, {location_employee_columns['work_location_id']})
    );
    """)


def create_employee_table(cursor) -> None:
    """Create the employees table."""
    employee_table = TABLES["employees"]["table_name"]
    employee_columns = TABLES["employees"]["columns"]
    cursor.execute(f"""
    CREATE TABLE {employee_table} (
        {employee_columns['id']} SERIAL PRIMARY KEY,
        {employee_columns['gender']} VARCHAR(1) NOT NULL,
        {employee_columns['age']} INTEGER NOT NULL,
        {employee_columns['contract_hours']} VARCHAR(255) NOT NULL,
        {employee_columns['in_training']} BOOLEAN NOT NULL,
        {employee_columns['expertise']} VARCHAR(255) NOT NULL,
        {employee_columns['hire_date']} DATE NOT NULL
    );
    """)

def create_attendance_table(cursor) -> None:
    """Create the attendance table."""
    attendance_table = TABLES["attendance"]["table_name"]
    attendance_columns = TABLES["attendance"]["columns"]
    cursor.execute(f"""
    CREATE TABLE {attendance_table} (
        {attendance_columns['id']} SERIAL PRIMARY KEY,
        {attendance_columns['employee_id']} INTEGER REFERENCES {TABLES['employees']['table_name']}({TABLES['employees']['columns']['id']}),
        {attendance_columns['date']} DATE NOT NULL
    );
    """)


def create_left_organisation_table(cursor) -> None:
    """Create the left organisation table with a one-to-one relationship to the employee table."""
    left_organisation_table = TABLES["left_organisation"]["table_name"]
    left_organisation_columns = TABLES["left_organisation"]["columns"]
    cursor.execute(f"""
    CREATE TABLE {left_organisation_table} (
        {left_organisation_columns['employee_id']} INTEGER NOT NULL UNIQUE REFERENCES {TABLES['employees']['table_name']}({TABLES['employees']['columns']['id']}),
        {left_organisation_columns['end_date']} DATE NOT NULL
    );
    """)