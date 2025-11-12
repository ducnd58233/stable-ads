import csv
import pandas as pd
from pathlib import Path
from typing import Iterator
from .base import FileParser

class CSVParser(FileParser):
    def can_parse(self, file_path: str) -> bool:
        return Path(file_path).suffix.lower() == '.csv'
    
    def get_headers(self, file_path: str) -> list[str]:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return reader.fieldnames or []
    
    def validate_fields(self, file_path: str, required_fields: list[str]) -> tuple[bool, list[str]]:
        headers = self.get_headers(file_path)
        missing = [field for field in required_fields if field not in headers]
        return (len(missing) == 0, missing)
    
    def read_file_bytes(self, file_path: str) -> bytes:
        with open(file_path, 'rb') as f:
            return f.read()
    
    def read_dataframe(self, file_path: str, fields: list[str] | None = None) -> pd.DataFrame:
        """Read entire CSV into DataFrame"""
        df = pd.read_csv(file_path, usecols=fields if fields else None, low_memory=False)
        return df
    
    def read_dataframe_chunks(
        self, 
        file_path: str, 
        fields: list[str] | None = None,
        chunksize: int = 10000
    ) -> Iterator[pd.DataFrame]:
        """Read CSV in chunks for large files"""
        return pd.read_csv(
            file_path, 
            usecols=fields if fields else None,
            chunksize=chunksize,
            low_memory=False
        )
    
    def transform(self, df: pd.DataFrame, fields: list[str]) -> pd.DataFrame:
        """
        CSV-specific transformations on top of base transformations.
        """
        # Call base transform first
        df = super().transform(df, fields)
        
        # CSV-specific: handle common CSV issues
        # Remove BOM if present
        if df.columns[0].startswith('\ufeff'):
            df.columns = [col.replace('\ufeff', '') for col in df.columns]
        
        # Handle quoted fields that might have extra whitespace
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.replace('"', '').str.strip()
        
        return df