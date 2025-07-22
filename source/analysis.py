import os
import warnings
import pandas as pd 

# just to avoid userwarnings from pandas when dealing with inferred formats
# in production this should be handled more gracefully
warnings.filterwarnings("ignore", category=UserWarning)

class GapAnalysis:
    """
    A class for analyzing viewing gaps in streaming service data to determine 
    subscription types (ad-supported vs ad-free).
    
    This class processes streaming data to identify gaps between viewing sessions,
    categorizes these gaps, and uses gap patterns to infer whether users have
    ad-supported or ad-free subscriptions.
    
    Attributes:
        path_to_data (str): Path to the data file.
        streaming_service (str): Name of the streaming service to analyze.
        application_column (str): Column name containing application data.
        df (pd.DataFrame): Filtered DataFrame containing only the specified streaming service data.
        gap_analysis_df (pd.DataFrame): DataFrame with calculated gaps between sessions.
        frequency_df (pd.DataFrame): DataFrame with gap frequency analysis.
    
    Example:
        >>> analyzer = GapAnalysis('./data/streaming.csv', 'Netflix')
        >>> subscription_types = analyzer.categorize_subscription_types()
        >>> print(subscription_types.head())
    """
    
    def __init__(self, 
                 path_to_data: str,
                 streaming_service: str,
                 application_column: str = 'application'):
        """
        Initialize the GapAnalysis with data loading and preprocessing.
        
        Args:
            path_to_data (str): Path to the CSV file containing streaming data.
            streaming_service (str): Name of the streaming service to filter and analyze.
            application_column (str, optional): Column name containing application data. 
                                              Defaults to 'application'.
        
        Raises:
            FileNotFoundError: If the specified data file path does not exist.
            ValueError: If the file format is not supported (non-CSV).
        """
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
        """
        Load data from a CSV file into a pandas DataFrame.
        
        Returns:
            pd.DataFrame: The loaded DataFrame from the CSV file.
            
        Raises:
            FileNotFoundError: If the specified file path does not exist.
            ValueError: If the file format is not CSV.
        """
        if not os.path.exists(self.path_to_data):
            raise FileNotFoundError(f"The file '{self.path_to_data}' was not found.")
        
        if self.path_to_data.endswith('.csv'):
            df = pd.read_csv(self.path_to_data, low_memory=False)
            print('Dataframe loaded successfully.')
            return df
        else:
            raise ValueError("Unsupported file format. Please provide a CSV file.")
        
    def _tv_counts_df(self,
                      tv_id_col: str = 'tv_id') -> pd.DataFrame:
        """
        Create a DataFrame with TV ID counts to identify TVs with multiple sessions.
        
        Args:
            tv_id_col (str, optional): Column name for TV identifier. Defaults to 'tv_id'.
            
        Returns:
            pd.DataFrame: DataFrame with TV IDs and their occurrence counts.
        """
        tv_counts = self.df[tv_id_col].value_counts().reset_index()
        tv_counts.columns = [tv_id_col, 'count']
        return tv_counts
    
    def _merge_tv_counts(self, 
                        tv_counts_df: pd.DataFrame, 
                        tv_id_col: str = 'tv_id') -> pd.DataFrame:
        """
        Merge TV counts with main DataFrame and filter to keep only TVs with multiple sessions.
        
        This method modifies self.df in place to include count information and removes
        TVs that have only one viewing session (as gaps cannot be calculated).
        
        Args:
            tv_counts_df (pd.DataFrame): DataFrame containing TV ID counts.
            tv_id_col (str, optional): Column name for TV identifier. Defaults to 'tv_id'.
            
        Returns:
            pd.DataFrame: The modified DataFrame (also updates self.df).
        """
        self.df = self.df.merge(tv_counts_df, on=tv_id_col, how='left')
        self.df = self.df[self.df['count'] > 1].reset_index(drop=True)
        
    def _create_session_id_col(self) -> pd.DataFrame:
        """
        Create a unique session identifier by combining TV ID and content ID.
        
        This method creates a 'tv_content_id' column that uniquely identifies
        each viewing session for gap analysis.
        
        Returns:
            pd.DataFrame: The modified DataFrame with new session ID column.
        """
        self.df['tv_content_id'] = self.df['tv_id'].astype(str) + '_' + self.df['content_id'].astype(str)
        
    def _create_gap_analysis_df(self) -> pd.DataFrame:
        """
        Create a comprehensive gap analysis DataFrame with calculated time gaps between sessions.
        
        This method processes each unique TV-content combination to:
        1. Sort sessions by start time
        2. Calculate gaps between consecutive sessions
        3. Convert gaps to seconds for analysis
        
        Returns:
            pd.DataFrame: DataFrame containing gap analysis with columns for gap times,
                         gap durations in seconds, and session information.
        """
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
        """
        Create a frequency distribution of gaps organized into time ranges.
        
        This method bins gap durations into 15-second intervals and counts
        the frequency of gaps in each range for each TV.
        
        Args:
            gap_analysis_df (pd.DataFrame): DataFrame containing gap analysis data.
            
        Returns:
            pd.DataFrame: DataFrame with gap ranges and their frequencies per TV.
        """
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
        """
        Categorize TV subscribers as having ad-supported, ad-free, or mixed subscriptions
        based on viewing gap patterns.
        
        This method analyzes gap patterns to infer subscription types:
        - Ad-supported: Frequent short gaps (≤60 seconds) indicating ad breaks
        - Ad-free: Predominantly longer gaps indicating natural viewing breaks
        - Mixed/Uncertain: Ambiguous patterns that don't clearly fit either category
        
        Args:
            ad_threshold (int, optional): Minimum number of ad-like gaps required 
                                        for ad-supported classification. Defaults to 3.
            ad_frequency_threshold (float, optional): Minimum proportion of ad-like gaps 
                                                    for ad-supported classification. Defaults to 0.6.
        
        Returns:
            pd.DataFrame: DataFrame containing subscription type analysis with columns:
                - tv_id: Television identifier
                - subscription_type: Inferred subscription type
                - total_gaps: Total number of gaps observed
                - ad_like_gaps: Number of short gaps (≤60 seconds)
                - long_gaps: Number of longer gaps (>60 seconds)
                - ad_gap_proportion: Proportion of gaps that are ad-like
                - most_common_ranges: Most frequent gap ranges for this TV
                
        Example:
            >>> results = analyzer.categorize_subscription_types(ad_threshold=5, ad_frequency_threshold=0.7)
            >>> print(results[results['subscription_type'] == 'ad_supported'].head())
        """
        def _extract_upper_bound(gap_range):
            """Extract the upper bound from a gap range string (e.g., '0-15' -> 15)."""
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