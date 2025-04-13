import psycopg2
from etl.file_operations import convert_excel_to_csv, read_csv_in_chunks
from etl.data_processing import transform_data
from etl.db_operations import create_tables, load_data
from config import DATABASE_CONFIG, SOURCE_RELATIVE_PATH

def source_to_OLTP(chunksize: int = 100000) -> None:
    """Execute the ETL process."""
    csv_file_path = SOURCE_RELATIVE_PATH.replace(".xlsx", ".csv")
    convert_excel_to_csv(SOURCE_RELATIVE_PATH, csv_file_path)

    with psycopg2.connect(**DATABASE_CONFIG) as connection:
        create_tables(connection)
        for chunk in read_csv_in_chunks(csv_file_path, chunksize=chunksize):
            employees, attendance = transform_data(chunk)
            load_data(connection, employees, attendance)


if __name__ == "__main__":
    source_to_OLTP()
