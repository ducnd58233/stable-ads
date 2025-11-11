from abc import ABC, abstractmethod
from typing import Any
import pandas as pd

class FileParser(ABC):
    @abstractmethod
    def can_parse(self, file_path: str) -> bool:
        """Check if this parser can handle the given file"""
        pass
    
    @abstractmethod
    def get_headers(self, file_path: str) -> list[str]:
        """Extract column headers from the file"""
        pass
    
    @abstractmethod
    def validate_fields(self, file_path: str, required_fields: list[str]) -> tuple[bool, list[str]]:
        """
        Validate that required fields exist in the file.
        Returns: (is_valid, missing_fields)
        """
        pass
    
    @abstractmethod
    def read_file_bytes(self, file_path: str) -> bytes:
        """Read file as bytes for upload"""
        pass
    
    @abstractmethod
    def read_dataframe(self, file_path: str, fields: list[str] | None = None) -> pd.DataFrame:
        """
        Read file into pandas DataFrame.
        For large files, this should return a chunked iterator or handle streaming.
        """
        pass
    
    @abstractmethod
    def read_dataframe_chunks(
        self, 
        file_path: str, 
        fields: list[str] | None = None,
        chunksize: int = 10000
    ) -> Any:  # Returns iterator of DataFrames
        """
        Read file in chunks for large files.
        Returns iterator of DataFrames.
        """
        pass
    
    def transform(self, df: pd.DataFrame, fields: list[str]) -> pd.DataFrame:
        """
        Apply general data cleaning and transformation best practices.
        Can be overridden by specific parsers for file-type-specific transformations.
        
        Best practices applied:
        - Remove duplicates
        - Handle missing values
        - Trim whitespace from string columns
        - Standardize date formats
        - Type inference and conversion
        """
        # Select only required fields
        if fields:
            df = df[fields]
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Trim whitespace from string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
            # Replace empty strings with NaN
            df[col] = df[col].replace('', pd.NA)
        
        # Standardize date columns (common patterns)
        date_patterns = ['date', 'time', 'timestamp', 'created', 'updated']
        for col in df.columns:
            if any(pattern in col.lower() for pattern in date_patterns):
                df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
        
        # Remove rows where all required fields are null
        if fields:
            df = df.dropna(subset=fields, how='all')
        
        # Type inference for numeric columns
        for col in df.select_dtypes(include=['object']).columns:
            if col not in [c for c in df.columns if 'date' in c.lower() or 'time' in c.lower()]:
                # Try to convert to numeric
                numeric = pd.to_numeric(df[col], errors='coerce')
                if not numeric.isna().all():
                    df[col] = numeric
        
        return df
    
    def transform_chunk(self, df: pd.DataFrame, fields: list[str]) -> pd.DataFrame:
        """
        Transform a single chunk. Same as transform() but optimized for chunked processing.
        """
        return self.transform(df, fields)