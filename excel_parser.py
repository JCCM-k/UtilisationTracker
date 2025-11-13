import pandas as pd
import io
from typing import Dict, List, Union
from pathlib import Path

class ExcelTableExtractor:
    """Extract multiple table blocks from a single Excel or CSV file."""
    
    def __init__(self, excel_path: Union[str, io.BytesIO], filename: str = None):
        """
        Initialize with either file path or BytesIO object
        
        Args:
            excel_path: File path string or BytesIO object containing file data
            filename: Optional filename (required when using BytesIO to detect file type)
        """
        if isinstance(excel_path, io.BytesIO):
            # Determine file type from filename
            if filename:
                if filename.lower().endswith('.csv'):
                    self.df = pd.read_csv(excel_path, header=None)
                elif filename.lower().endswith(('.xlsx', '.xls')):
                    self.df = pd.read_excel(excel_path, header=None, engine='openpyxl')
                else:
                    raise ValueError(f"Unsupported file format: {filename}")
            else:
                # Default to Excel if no filename provided
                self.df = pd.read_excel(excel_path, header=None, engine='openpyxl')
        else:
            # Read from file path (original behavior)
            path_str = str(excel_path)
            if path_str.lower().endswith('.csv'):
                self.df = pd.read_csv(excel_path, header=None)
            elif path_str.lower().endswith(('.xlsx', '.xls')):
                self.df = pd.read_excel(excel_path, header=None, engine='openpyxl')
            else:
                raise ValueError(f"Unsupported file format: {excel_path}")
        
        self.tables = {}
    
    @classmethod
    def from_bytes(cls, file_bytes: io.BytesIO, filename: str):
        """
        Factory method to create instance from BytesIO
        
        Args:
            file_bytes: BytesIO object containing file data
            filename: Filename to determine file type (.csv, .xlsx, .xls)
        """
        return cls(file_bytes, filename)
    
    def _find_boundaries(self, headings: List[str]) -> Dict[str, tuple]:
        """Find start and end rows for each table heading."""
        boundaries = {}
        for heading in headings:
            if not (self.df[0] == heading).any():
                continue
            
            start = self.df[self.df[0] == heading].index[0]
            end = start + 1
            empty_count = 0
            
            for i in range(start + 1, len(self.df)):
                if pd.isna(self.df.iloc[i, 0]):
                    empty_count += 1
                    if empty_count >= 2:
                        end = i - 2
                        break
                else:
                    empty_count = 0
            
            boundaries[heading] = (start, end)
        
        return boundaries
    
    def _extract_table(self, start: int, end: int, offset: int = 1) -> pd.DataFrame:
        """Extract a single table from the raw dataframe."""
        header_idx = start + offset
        headers = self.df.iloc[header_idx].values
        cols = [i for i, h in enumerate(headers) if pd.notna(h)]
        
        df = self.df.iloc[header_idx + 1:end + 1, cols].copy()
        df.columns = [str(headers[i]).strip() for i in cols]
        
        return df.dropna(how='all').reset_index(drop=True)
    
    def extract_all(self, headings: List[str] = None) -> Dict[str, pd.DataFrame]:
        """Extract all tables and return as dictionary."""
        if headings is None:
            headings = ['Project Timeline', 'Hours Analysis by Module',
                       'Rate Calculation', 'Cost Analysis by Step']
        
        boundaries = self._find_boundaries(headings)
        self.tables = {h: self._extract_table(s, e) for h, (s, e) in boundaries.items()}
        
        return self.tables
