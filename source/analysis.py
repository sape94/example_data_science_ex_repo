import os
import pandas as pd 

class GapAnalysis:
    
    def __init__(self, 
                 path_to_data: str,
                 streaming_service: str,
                 application_column: str = 'application'):
        self.path_to_data = path_to_data
        
        df = self.load_data(path_to_data)
        self.df = df[df[application_column].isin(streaming_service)].reset_index(drop=True)
        
        self.streaming_service = streaming_service
        self.application_column = application_column
        
    def load_data(file_path: str) -> pd.DataFrame:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file '{file_path}' was not found.")
        
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            print('Dataframe loaded successfully.')
            return df
        else:
            raise ValueError("Unsupported file format. Please provide a CSV file.")
        