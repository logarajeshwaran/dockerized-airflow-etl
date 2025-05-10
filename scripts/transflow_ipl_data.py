import pandas as pd


def read_data_frame(file_path):
    """
    Read a CSV file and return a DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        print(f"Data read successfully from {file_path}")
        return df
    except Exception as e:
        print(f"Error reading data from {file_path}: {e}")
        return None


def mysql_transform(file_path):
    """
    Transform the DataFrame for MySQL.
    """
    try:
        df_mysql = read_data_frame(file_path)
        if df_mysql is None:
            return None
        # Example transformation: Convert all column names to lowercase
        df_mysql['toss_decision_impact'] = (df_mysql['toss_winner'] == df_mysql['winner']).astype(int)
        mysql_records = df_mysql.to_dict('records')
        print("Data transformed successfully for MySQL")
        return mysql_records
    except Exception as e:
        print(f"Error transforming data for MySQL: {e}")
        return None
    
def mongo_transform(file_path):
    """
    Transform the DataFrame for MongoDB.
    """
    try:
        df_mongo = read_data_frame(file_path)
        if df_mongo is None:
            return None
        # Example transformation: Convert all column names to lowercase
        df_mongo['is_upset'] = (df_mongo['team1'] != df_mongo['winner']) & (df_mongo['team2'] != df_mongo['winner'])
        df_mongo['venue_type'] = df_mongo['venue'].apply(lambda x: 'international' if len(x.split()) > 1 else 'domestic')
        df_mongo['teams'] = df_mongo.apply(lambda x: {'team1': x['team1'], 'team2': x['team2']}, axis=1)
        df_mongo.drop(['team1', 'team2'], axis=1, inplace=True)
        mongo_records = df_mongo.to_dict('records')
        print("Data transformed successfully for MongoDB")
        return mongo_records
    except Exception as e:
        print(f"Error transforming data for MongoDB: {e}")
        return None
    