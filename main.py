from source.analysis import GapAnalysis


if __name__ == "__main__":
    path = './data/data.csv'
    streaming_service = 'Netflix'
    gap_instance = GapAnalysis(path_to_data=path,
                               streaming_service=streaming_service)
    print(gap_instance.df.head())
    print(gap_instance.gap_analysis_df.head())
    print(gap_instance.frequency_df.head())
    print(gap_instance.categorize_subscription_types())
