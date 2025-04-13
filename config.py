DATABASE_CONFIG = {
    "dbname": "INDATAD_b2",
    "user": "group_b2",
    "password": "ZPwwg0z0v1LKx1t6YK6D",
    "host": "95.217.3.61",
    "port": "5432"
}

SOURCE_RELATIVE_PATH = "data/test_employees.xlsx"

# Define mappings for source data columns to internal field names
COLUMN_MAPPING = {
    "gender": "Geslacht",
    "age": "Leeftijd",
    "contract_hours": "Dienstverband/uren contract",
    "in_training": "In opleiding",
    "is_absent": "Verzuim",
    "expertise": "Deskundigheid",
    "hire_date": "Datum in dienst",
    "date": "Datum",
    "end_date": "Datum uit dienst",
    "work_location": "Werklocatie"
}

# Define table and column names in the database
# table_name: "..." change "..." with whatever name you would like for that table
# First attribute in the colums is from the collum_mapping (shouldn't change), Second attribute is for changing the name in the database (feel free to change)
TABLES = {
    "employees": {
        "table_name": "medewerker",
        "columns": {
            "id": "id",
            "gender": "geslacht",
            "age": "leeftijd",
            "contract_hours": "dienstverband",
            "in_training": "in_opleiding",
            "expertise": "deskundigheid",
            "hire_date": "datum_in_dienst"
        },
    },
    "attendance": {
        "table_name": "verzuim",
        "columns": {
            "id": "id",
            "employee_id": "medewerker_id",
            "date": "datum",
        },
    },
    "left_organisation": {
        "table_name": "uitdienst",
        "columns": {
            "employee_id": "medewerker_id",
            "end_date": "datum_uit_dienst"
        },
    },
    "work_location_employee": {
        "table_name": "werk_locatie_medewerker",
        "columns": {
            "work_location_id": "werk_locatie_id",
            "employee_id": "medewerker_id"
        }
    },
    "work_location": {
        "table_name": "werk_locatie",
        "columns": {
            "id": "id",
            "work_location": "naam",
            "address": "straatnaam",
            "postal_code": "postcode"
        }
    }
}


SOURCE_DATE_FORMAT = "%d-%m-%Y"
