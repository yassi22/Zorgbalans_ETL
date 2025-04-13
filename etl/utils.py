from typing import List, Dict, Any

def deduplicate_employees(employees: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate employees from the list."""
    seen = set()
    unique_employees = []
    for employee in employees:
        employee_tuple = tuple(employee.items())  # Includes 'work_location'
        if employee_tuple not in seen:
            seen.add(employee_tuple)
            unique_employees.append(employee)
    return unique_employees

