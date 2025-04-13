import pandas as pd
from typing import List, Dict, Tuple
from config import COLUMN_MAPPING, SOURCE_DATE_FORMAT


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Clean column names to remove leading/trailing spaces and standardize them."""
    df.columns = df.columns.str.strip()
    return df

def validate_columns(df: pd.DataFrame) -> None:
    """Validate that all required columns are present in the DataFrame."""
    missing_columns = [source for source in COLUMN_MAPPING.values() if source not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in Excel: {', '.join(missing_columns)}")

def transform_data(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
    """Transform Excel data into normalized format."""
    validate_columns(df)
    rename_columns(df)
    convert_column_types(df)
    employees, attendance = process_records(df)
    return employees, attendance

def rename_columns(df: pd.DataFrame) -> None:
    """Map Excel columns to internal names."""
    df.rename(columns={source: internal for internal, source in COLUMN_MAPPING.items()}, inplace=True)

def convert_column_types(df: pd.DataFrame) -> None:
    """Convert columns to appropriate types."""
    for date_col in ["hire_date", "end_date", "date"]:
        df[date_col] = pd.to_datetime(df[date_col], format=SOURCE_DATE_FORMAT, errors="coerce").dt.date
    df["in_training"] = df["in_training"].map({"ja": True, "nee": False})
    df["is_absent"] = df["is_absent"].astype(bool)

def process_records(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
    """Process records into employee and attendance data."""
    employees = {}
    attendance = []

    for idx, row in df.iterrows():
        employee_key = {key: row[key] for key in [
            "gender", "age", "contract_hours", "in_training",
            "expertise", "work_location", "hire_date", "end_date"
        ]}
        key_tuple = tuple(employee_key.items())
        if key_tuple not in employees:
            employees[key_tuple] = employee_key
        attendance.append({
            "employee_key": employee_key,
            "date": row["date"],
            "is_absent": row["is_absent"],
        })
    return list(employees.values()), attendance

