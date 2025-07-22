import os
import warnings
import pandas as pd 

# just to avoid userwarnings from pandas when dealing with inferred formats
# in production this should be handled more gracefully
warnings.filterwarnings("ignore", category=UserWarning)

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
        
        self._merge_tv_counts(self._tv_counts_df())
        self._create_session_id_col()
        
        self.gap_analysis_df = self._create_gap_analysis_df()
        
        self.frequency_df = self._create_gap_frequency_df(self.gap_analysis_df)
        
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
    
    def _merge_tv_counts(self, 
                        tv_counts_df: pd.DataFrame, 
                        tv_id_col: str = 'tv_id') -> pd.DataFrame:
        self.df = self.df.merge(tv_counts_df, on=tv_id_col, how='left')
        self.df = self.df[self.df['count'] > 1].reset_index(drop=True)
        
    def _create_session_id_col(self) -> pd.DataFrame:
        self.df['tv_content_id'] = self.df['tv_id'].astype(str) + '_' + self.df['content_id'].astype(str)
        
    def _create_gap_analysis_df(self) -> pd.DataFrame:
        sub_dfs = []
        for value in self.df['tv_content_id'].unique():
            sub_df = self.df[self.df['tv_content_id'] == value].reset_index(drop=True)
            if len(sub_df) > 1:
                sub_df = sub_df[['tv_content_id', 'tv_id', 'content_id', 'start_time', 'end_time', 'duration', 'title', 'season_id']].sort_values(by='start_time', ascending=True).reset_index(drop=True)
                sub_df['start_time'] = sub_df['start_time'].str.strip()
                sub_df['start_time'] = pd.to_datetime(sub_df['start_time'])
                sub_df['end_time'] = sub_df['end_time'].str.strip()
                sub_df['end_time'] = pd.to_datetime(sub_df['end_time'])
                sub_df['gap_vs_previous_session'] = sub_df['start_time'] - sub_df['end_time'].shift()
                sub_df['gap_seconds'] = pd.to_timedelta(sub_df['gap_vs_previous_session']).dt.total_seconds()
                sub_dfs.append(sub_df)
        gap_analysis_df = pd.concat(sub_dfs, ignore_index=True)
        return gap_analysis_df
    
    def _create_gap_frequency_df(self,
                                 gap_analysis_df: pd.DataFrame) -> pd.DataFrame:
        df_clean = gap_analysis_df.dropna(subset=['gap_seconds']).copy()
    
        max_gap = df_clean['gap_seconds'].max()
        if pd.isna(max_gap) or max_gap < 0:
            max_gap = 60
        
        bin_edges = list(range(0, int(max_gap) + 15, 15))
        gap_labels = [f"{bin_edges[i]}-{bin_edges[i+1]}" for i in range(len(bin_edges) - 1)]
        
        df_clean['gap_range'] = pd.cut(
            df_clean['gap_seconds'], 
            bins=bin_edges, 
            labels=gap_labels, 
            right=False,
            include_lowest=True
        )
        
        frequency_df = (df_clean
                    .groupby(['tv_id', 'gap_range'], observed=True)
                    .size()
                    .reset_index(name='frequency'))
        
        return frequency_df
    
    def categorize_subscription_types(self, 
                                      ad_threshold=3, 
                                      ad_frequency_threshold=0.6):
        def _extract_upper_bound(gap_range):
            return int(gap_range.split('-')[1])
        
        self.frequency_df = self.frequency_df.copy()
        self.frequency_df['gap_upper_bound'] = self.frequency_df['gap_range'].apply(_extract_upper_bound)
        
        # ad-like gaps (less than 60 seconds) based on google search for ads' time on netflix or hulu
        self.frequency_df['is_ad_gap'] = self.frequency_df['gap_upper_bound'] <= 60
        
        tv_metrics = []
        
        for tv_id in self.frequency_df['tv_id'].unique():
            tv_data = self.frequency_df[self.frequency_df['tv_id'] == tv_id]
            
            total_gaps = tv_data['frequency'].sum()
            ad_gaps = tv_data[tv_data['is_ad_gap']]['frequency'].sum()
            long_gaps = tv_data[~tv_data['is_ad_gap']]['frequency'].sum()
            
            ad_gap_proportion = ad_gaps / total_gaps if total_gaps > 0 else 0
            
            most_common = tv_data.nlargest(3, 'frequency')['gap_range'].tolist()
            
            if total_gaps == 0:
                subscription_type = 'insufficient_data'
            elif ad_gaps >= ad_threshold and ad_gap_proportion >= ad_frequency_threshold:
                subscription_type = 'ad_supported'
            elif ad_gap_proportion < 0.3 and long_gaps > ad_gaps:
                #long gaps, likely natural breaks
                subscription_type = 'ad_free'
            elif ad_gaps < 2:
                subscription_type = 'ad_free' 
            else:
                subscription_type = 'mixed_or_uncertain'
            
            tv_metrics.append({
                'tv_id': tv_id,
                'subscription_type': subscription_type,
                'total_gaps': total_gaps,
                'ad_like_gaps': ad_gaps,
                'long_gaps': long_gaps,
                'ad_gap_proportion': round(ad_gap_proportion, 3),
                'most_common_ranges': ', '.join(most_common[:3])
            })
        
        return pd.DataFrame(tv_metrics)