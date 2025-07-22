import pandas as pd
import re
import os

def load_data(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' was not found.")
    
    df = pd.read_csv(file_path)
    print('Dataframe loaded successfully.')
    return df

def display_dataframe_info(df: pd.DataFrame):
    print('----------------------------')
    print('First few rows of the Dataframe:')
    print(df.head())

    print('----------------------------')
    print('Dataframe shape:', df.shape)

    print('----------------------------')
    print('Dataframe info:')
    df.info()

def get_unique_list_from_column(df: pd.DataFrame, 
                                column_name: str) -> list:
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")
        
    unique_list = df[column_name].unique().tolist()
    print(f'Dataframe {column_name} unique values:', unique_list)
    return unique_list

def find_exact_word_case_insensitive(string_list: list, 
                                     word: str) -> list:
    # Ensure all elements in the list are treated as strings
    string_list_str = [f'{s}' for s in string_list]
    regex = re.compile(fr"^{re.escape(word)}$", re.IGNORECASE)
    
    matching_elements = [s for s in string_list_str if regex.match(s)]
    return matching_elements

def analyze_streaming_services(df: pd.DataFrame):
    print('----------------------------')
    app_list = get_unique_list_from_column(df, 'application')

    print('----------------------------')
    network_list = get_unique_list_from_column(df, 'network')

    print('----------------------------')
    found_app_case_insensitive_netflix = find_exact_word_case_insensitive(app_list, 'netflix')
    print('Elements from application list that match Netflix (case-insensitive):', found_app_case_insensitive_netflix)

    print('----------------------------')
    found_app_case_insensitive_hulu = find_exact_word_case_insensitive(app_list, 'hulu')
    print('Elements from application list that match Hulu (case-insensitive):', found_app_case_insensitive_hulu)

    print('----------------------------')
    found_network_case_insensitive_netflix = find_exact_word_case_insensitive(network_list, 'netflix')
    print('Elements from network list that match Netflix (case-insensitive):', found_network_case_insensitive_netflix)

    print('----------------------------')
    found_network_case_insensitive_hulu = find_exact_word_case_insensitive(network_list, 'hulu')
    print('Elements from network list that match Hulu (case-insensitive):', found_network_case_insensitive_hulu)


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)

    data_file_path = './data/data.csv' 

    try:
        main_df = load_data(data_file_path)

        display_dataframe_info(main_df)

        analyze_streaming_services(main_df)

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")