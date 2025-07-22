from source.analysis import GapAnalysis


if __name__ == "__main__":
    path = './data/data.csv'
    streaming_service = 'Netflix'
    gap_instance = GapAnalysis(path_to_data=path,
                               streaming_service=streaming_service)
    gap_instance.merge_tv_counts(gap_instance._tv_counts_df())
    print(gap_instance.df.head())
