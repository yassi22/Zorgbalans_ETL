from psycopg2.extras import execute_batch
from typing import List, Dict, Any, Tuple
from config import TABLES
from scraper_locations import scrape_locations
from pandas import isnull

def insert_work_locations(cursor, employees: List[Dict[str, Any]]) -> Dict[str, int]:
    """Insert unique work locations into the work_locations table."""
    unique_locations = set()
    for emp in employees:
        work_locations = emp["work_location"].split(", ")  # Split multiple locations
        unique_locations.update(work_locations)  # Add to the unique set

    location_map = {}
    scraped_locations = scrape_locations()

    for location in unique_locations:
        full_location = next((loc for loc in scraped_locations if loc["name"] == location), None)
        if not full_location:
            print(f"Warning: No details found for location '{location}'")
            full_location = {
                "name": location,
                "street": "Not Found",
                "postal_code": "Not Found"
            }

        cursor.execute(
            f"""
            INSERT INTO {TABLES['work_location']['table_name']} 
            ({TABLES['work_location']['columns']['work_location']}, {TABLES['work_location']['columns']['address']}, {TABLES['work_location']['columns']['postal_code']})
            VALUES (%(name)s, %(street)s, %(postal_code)s)
            ON CONFLICT ({TABLES['work_location']['columns']['work_location']})
            DO NOTHING RETURNING {TABLES['work_location']['columns']['id']};
            """,
            full_location
        )
        location_id = cursor.fetchone()
        if location_id:
            location_map[location] = location_id[0]

    # Fallback to get existing location IDs if not inserted
    for location in unique_locations:
        if location not in location_map:
            cursor.execute(
                f"""
                SELECT {TABLES['work_location']['columns']['id']} 
                FROM {TABLES['work_location']['table_name']} 
                WHERE {TABLES['work_location']['columns']['work_location']} = %s;
                """,
                (location,)
            )
            location_map[location] = cursor.fetchone()[0]

    return location_map


def insert_work_location_employee(cursor, employees: List[Dict], location_map: Dict[str, int], employee_ids: Dict[Tuple, int]) -> None:
    """Insert employee-work_location relationships into the junction table."""
    query = f"""
    INSERT INTO {TABLES['work_location_employee']['table_name']} ({TABLES['work_location_employee']['columns']['employee_id']}, {TABLES['work_location_employee']['columns']['work_location_id']})
    VALUES (%s, %s)
    ON CONFLICT DO NOTHING;
    """

    relations = []
    for emp in employees:
        # Get employee_id from employee_ids map
        emp_key = tuple(emp.items())
        employee_id = employee_ids.get(emp_key)
        if not employee_id:
            print(f"Warning: Employee ID not found for: {emp}")
            continue

        # Get work_location IDs
        work_locations = emp["work_location"].split(", ")
        for location in work_locations:
            work_location_id = location_map.get(location)
            if work_location_id:
                relations.append((employee_id, work_location_id))
            else:
                print(f"Warning: Location ID not found for: {location}")

    # Bulk insert relationships
    if relations:
        execute_batch(cursor, query, relations, page_size=1000)



def insert_employees(cursor, employees: List[Dict], location_map: Dict[str, int]) -> Dict[Tuple, int]:
    """Insert employees and handle left organisation records for non-null datum_uit_dienst."""
    employee_query = f"""
    INSERT INTO {TABLES['employees']['table_name']}
    ({', '.join([TABLES['employees']['columns'][col] for col in ['gender', 'age', 'contract_hours', 'in_training', 'expertise', 'hire_date']])})
    VALUES (%(gender)s, %(age)s, %(contract_hours)s, %(in_training)s, %(expertise)s, %(hire_date)s)
    RETURNING id;
    """

    left_organisation_query = f"""
    INSERT INTO {TABLES['left_organisation']['table_name']} ({TABLES['left_organisation']['columns']['employee_id']}, {TABLES['left_organisation']['columns']['end_date']})
    VALUES (%s, %s);
    """

    employee_map = {}
    for emp in employees:
        try:
            # Make a copy of the employee dictionary
            trimmed_emp = emp.copy()
            end_date = trimmed_emp.pop("end_date", None)  # Safely pop end_date

            # Insert the employee and fetch the ID
            cursor.execute(employee_query, trimmed_emp)
            emp_id = cursor.fetchone()[0]

            # Update the employee map
            emp_key = tuple(emp.items())
            employee_map[emp_key] = emp_id

            # Insert into left_organisation table if end_date is valid
            if end_date and not isnull(end_date):
                cursor.execute(left_organisation_query, (emp_id, end_date))

        except Exception as e:
            print(f"Error inserting employee: {emp}\n{e}")
            cursor.connection.rollback()

    return employee_map

def insert_attendance(cursor, attendance: List[Dict]) -> None:
    """Insert attendance records into the database."""
    if not attendance:
        print("No valid attendance records to insert.")
        return


    valid_attendance: List[Dict] = []
    for record in attendance:
        if record["is_absent"]:
            valid_attendance.append(record)
    #valid_attendance

    attendance_query = f"""
    INSERT INTO {TABLES['attendance']['table_name']}
    ({TABLES['attendance']['columns']['employee_id']}, {TABLES['attendance']['columns']['date']})
    VALUES (%(employee_id)s, %(date)s);
    """
    try:
        execute_batch(cursor, attendance_query, valid_attendance, page_size=1000)
    except Exception as e:
        print(f"Error inserting attendance records: {e}")