import os
import pandas as pd 

class GapAnalysis:
    
    def __init__(self, 
                 path_to_data: str,
                 streaming_service: str,
                 application_column: str = 'application'):
        self.path_to_data = path_to_data
        
        df = self._load_data()
        self.df = df[df[application_column].isin([streaming_service])].reset_index(drop=True)
        
        self.streaming_service = streaming_service
        self.application_column = application_column
        
    def _load_data(self) -> pd.DataFrame:
        if not os.path.exists(self.path_to_data):
            raise FileNotFoundError(f"The file '{self.path_to_data}' was not found.")
        
        if self.path_to_data.endswith('.csv'):
            df = pd.read_csv(self.path_to_data)
            print('Dataframe loaded successfully.')
            return df
        else:
            raise ValueError("Unsupported file format. Please provide a CSV file.")
        
    def _tv_counts_df(self,
                      tv_id_col: str = 'tv_id') -> pd.DataFrame:
        tv_counts = self.df[tv_id_col].value_counts().reset_index()
        tv_counts.columns = [tv_id_col, 'count']
        return tv_counts
    
    def merge_tv_counts(self, 
                        tv_counts_df: pd.DataFrame, 
                        tv_id_col: str = 'tv_id') -> pd.DataFrame:
        self.df = self.df.merge(tv_counts_df, on=tv_id_col, how='left')
        self.df = self.df[self.df['count'] > 1].reset_index(drop=True)
        