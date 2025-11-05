import pandas as pd

from typing import Dict, List
from pathlib import Path

class ExcelTableExtractor:
    """Extract multiple table blocks from a single Excel sheet."""
    
    def __init__(self, excel_path: str):
        self.df = pd.read_excel(excel_path, header=None, engine='openpyxl')
        self.tables = {}
    
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
    
    def save_all(self, output_dir: str = '.', prefix: str = '') -> None:
        """Save all tables as CSV files."""
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        for name, df in self.tables.items():
            filename = f"{prefix}{name.lower().replace(' ', '_')}.csv"
            df.to_csv(Path(output_dir) / filename, index=False)

# Extract tables
extractor = ExcelTableExtractor('module.xlsx')
tables = extractor.extract_all()

# Access individual tables
project_timeline = tables['Project Timeline']
hours_analysis = tables['Hours Analysis by Module']
rate_calculation = tables['Rate Calculation']
cost_analysis = tables['Cost Analysis by Step']

# Save to CSV
extractor.save_all(prefix='tbl_')