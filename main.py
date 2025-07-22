from source.analysis import GapAnalysis


if __name__ == "__main__":
    path = './data/data.csv'
    output_path = './output/'
    streaming_services_list = ['Netflix', 'Hulu']
    for streaming_service in streaming_services_list:
        print(f'Analyzing data for {streaming_service}...')
        gap_instance = GapAnalysis(path_to_data=path,
                                   streaming_service=streaming_service)
        
        gap_instance.df.to_csv(f'{output_path}{streaming_service}_data.csv', index=False)
        
        
        gap_instance.gap_analysis_df.to_csv(f'{output_path}{streaming_service}_gap_analysis.csv', index=False)
        
        
        gap_instance.frequency_df.to_csv(f'{output_path}{streaming_service}_frequency_analysis.csv', index=False)
        
        
        subscription_types = gap_instance.categorize_subscription_types()
        subscription_types.to_csv(f'{output_path}{streaming_service}_subscription_types.csv', index=False)
        print(subscription_types.head())
