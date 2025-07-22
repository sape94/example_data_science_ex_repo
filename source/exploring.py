import pandas as pd
import re
import os

def load_data(file_path: str) -> pd.DataFrame:
    """
    Load data from a CSV file into a pandas DataFrame.
    
    Args:
        file_path (str): Path to the CSV file to be loaded.
        
    Returns:
        pd.DataFrame: The loaded DataFrame from the CSV file.
        
    Raises:
        FileNotFoundError: If the specified file path does not exist.
        
    Example:
        >>> df = load_data('./data/sample.csv')
        Dataframe loaded successfully.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' was not found.")
    
    df = pd.read_csv(file_path)
    print('Dataframe loaded successfully.')
    return df

def display_dataframe_info(df: pd.DataFrame):
    """
    Display comprehensive information about a DataFrame including shape, 
    first few rows, and general info.
    
    Args:
        df (pd.DataFrame): The DataFrame to analyze and display information for.
        
    Returns:
        None: This function prints information directly to the console.
        
    Example:
        >>> display_dataframe_info(my_df)
        ----------------------------
        First few rows of the Dataframe:
        [displays head of DataFrame]
        ----------------------------
        Dataframe shape: (100, 5)
        ----------------------------
        Dataframe info:
        [displays DataFrame.info() output]
    """
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
    """
    Extract unique values from a specified column in a DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame to extract unique values from.
        column_name (str): The name of the column to get unique values from.
        
    Returns:
        list: A list containing all unique values from the specified column.
        
    Raises:
        ValueError: If the specified column name is not found in the DataFrame.
        
    Example:
        >>> unique_vals = get_unique_list_from_column(df, 'category')
        Dataframe category unique values: ['A', 'B', 'C']
        >>> print(unique_vals)
        ['A', 'B', 'C']
    """
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")
        
    unique_list = df[column_name].unique().tolist()
    print(f'Dataframe {column_name} unique values:', unique_list)
    return unique_list

def find_exact_word_case_insensitive(string_list: list, 
                                     word: str) -> list:
    """
    Find all elements in a list that exactly match a given word (case-insensitive).
    
    This function performs an exact word match using regular expressions,
    ignoring case sensitivity. It treats all list elements as strings.
    
    Args:
        string_list (list): List of elements to search through (converted to strings).
        word (str): The word to search for (exact match, case-insensitive).
        
    Returns:
        list: A list of elements from string_list that exactly match the word
              (case-insensitive).
              
    Example:
        >>> items = ['Netflix', 'NETFLIX', 'Hulu', 'netflix', 'Prime']
        >>> matches = find_exact_word_case_insensitive(items, 'netflix')
        >>> print(matches)
        ['Netflix', 'NETFLIX', 'netflix']
    """
    # Ensure all elements in the list are treated as strings
    string_list_str = [f'{s}' for s in string_list]
    regex = re.compile(fr"^{re.escape(word)}$", re.IGNORECASE)
    
    matching_elements = [s for s in string_list_str if regex.match(s)]
    return matching_elements

def analyze_streaming_services(df: pd.DataFrame):
    """
    Analyze streaming service data by examining application and network columns.
    
    This function performs a comprehensive analysis of streaming services by:
    1. Extracting unique values from 'application' and 'network' columns
    2. Searching for Netflix and Hulu matches in both columns (case-insensitive)
    3. Displaying all results with formatted output
    
    Args:
        df (pd.DataFrame): DataFrame containing streaming service data with 
                          'application' and 'network' columns.
                          
    Returns:
        None: This function prints analysis results directly to the console.
        
    Raises:
        ValueError: If required columns ('application' or 'network') are not 
                   found in the DataFrame.
                   
    Example:
        >>> analyze_streaming_services(streaming_df)
        ----------------------------
        Dataframe application unique values: ['Netflix', 'Hulu', 'Prime']
        ----------------------------
        Dataframe network unique values: ['Netflix', 'HBO', 'Disney+']
        ----------------------------
        Elements from application list that match Netflix (case-insensitive): ['Netflix']
        [... additional output ...]
    """
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