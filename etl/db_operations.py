from typing import List, Dict, Any
from config import TABLES
from .db_create_tables import create_left_organisation_table, create_attendance_table, create_work_location_employee_table, create_employee_table, create_work_locations_table
from .db_insert_data import insert_employees, insert_attendance, insert_work_location_employee, insert_work_locations


def create_tables(connection) -> None:
    """Drop and recreate tables in the PostgreSQL database."""
    with connection.cursor() as cursor:
        drop_existing_tables(cursor)
        create_work_locations_table(cursor)
        create_employee_table(cursor)
        create_work_location_employee_table(cursor)
        create_attendance_table(cursor)
        create_left_organisation_table(cursor)
        connection.commit()


def drop_existing_tables(cursor) -> None:
    """Drop existing tables if they exist."""
    cursor.execute(f"DROP TABLE IF EXISTS {TABLES['attendance']['table_name']};")
    cursor.execute(f"DROP TABLE IF EXISTS werk_locatie_medewerker CASCADE;")
    cursor.execute(f"DROP TABLE IF EXISTS {TABLES['left_organisation']['table_name']} CASCADE;")
    cursor.execute(f"DROP TABLE IF EXISTS {TABLES['employees']['table_name']};")
    cursor.execute(f"DROP TABLE IF EXISTS {TABLES['work_location']['table_name']};")



def load_data(connection, employees: List[Dict[str, Any]], attendance: List[Dict[str, Any]]) -> None:
    """Load transformed data into the PostgreSQL database."""
    with connection.cursor() as cursor:
        location_map = insert_work_locations(cursor, employees)

        employee_map = insert_employees(cursor, employees, location_map)

        insert_work_location_employee(cursor, employees, location_map, employee_map)

        valid_attendance = map_attendance_to_employee(cursor, attendance, employee_map)
        insert_attendance(cursor, valid_attendance)

        connection.commit()


def map_attendance_to_employee(cursor, attendance: List[Dict], employee_map: Dict) -> List[Dict]:
    """Map attendance records to employee IDs."""
    valid_attendance = []
    for record in attendance:
        record["employee_key"].pop("work_location_id", None)  # Safely removes the key if it exists
        key = tuple(record["employee_key"].items())  # Includes 'work_location'
        record["employee_id"] = employee_map.get(key)
        if record["employee_id"]:
            del record["employee_key"]
            valid_attendance.append(record)
        else:
            print(f"Warning: No matching employee for attendance record: {record}")
    # print(f"Mapped {len(valid_attendance)} valid attendance records out of {len(attendance)} total.")
    return valid_attendance
