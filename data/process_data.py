# import libraries
import sys
import pandas as pd
from sqlalchemy import create_engine

def load_data(messages_filepath, categories_filepath):
    """
    This function loads csv files, merges them and returns a dataframe
    
    Args(strings):
        1. 'messages_filepath': filepath to csv file containing messages (text)
        2. 'categories_filepath': filepath to csv file containing categories of disasters (labels)
        
    Returns(pandas dataframe):
        1. df: merged dataset
    """
    
    # load messages
    messages = pd.read_csv(messages_filepath, index_col='id')
    # load categories
    categories = pd.read_csv(categories_filepath, index_col='id')
    
    # merge and return dataframe
    return pd.concat([messages, categories], axis=1)


def clean_data(df):
    """
    Creates dummy variables for each type of disaster, extracts their respective labels, and elimitates duplicates
    
    Arg (dataframe):
        1. df: dataframe with loaded data
    Returns (dataframe):
        1. df: dataframe with cleaned data
    """
    # create a dataframe of the 36 individual category columns
    categories = df['categories'].str.split(";", expand=True)

    # select the first row of the categories dataframe
    row = categories.iloc[0] 
    # use this row to extract a list of new column names for categories.
    category_colnames = row.str.split("-").str.get(0)
    # rename the columns of `categories`
    categories.columns = category_colnames.tolist()
    
    for column in categories:
    # set each value to be the last character of the string
        categories[column] = categories[column].str.split("-").str.get(-1)
    
    # convert column from string to numeric
        categories[column] = categories[column].astype(int)
    
    # drop the original categories column from `df`
    df.drop('categories', inplace=True, axis=1)
    
    # concatenate df with categories
    df = pd.concat([df, categories], axis =1 )
    
    # check number of duplicates
    indexes = df.index.value_counts() > 1
    duplicated_index_list = indexes[indexes == True].index
    return df[~df.index.duplicated(keep='first')]

def save_data(df, database_filename):
    """
    Store data in an sqlite database
    
    Args:
        1. df: Pandas dataframe with clean data
        2. database_filename: string with name of database
    """
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql(database_filename, engine, index=False, if_exists='replace')  


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()