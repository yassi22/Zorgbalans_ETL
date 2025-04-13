import pandas as pd
from typing import List

def convert_excel_to_csv(excel_file_path: str, csv_file_path: str) -> None:
    """Convert an Excel file to a CSV file."""
    df = pd.read_excel(excel_file_path)
    df.to_csv(csv_file_path, index=False)

def read_csv_in_chunks(file_path: str, chunksize: int) -> List[pd.DataFrame]:
    """Read a CSV file in chunks."""
    return [chunk for chunk in pd.read_csv(file_path, chunksize=chunksize)]
