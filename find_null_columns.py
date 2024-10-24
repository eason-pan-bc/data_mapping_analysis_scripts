import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def get_connection_string() -> str:
    """
    Creates connection string from environment variables.
    """
    load_dotenv()  # Load environment variables from .env file
    
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    service = os.getenv('DB_SERVICE')
    
    if not all([user, password, host, port, service]):
        raise ValueError("Missing required database configuration in .env file")
        
    return f"oracle://{user}:{password}@{host}:{port}/{service}"

def analyze_table_nulls() -> pd.DataFrame:
    """
    Analyzes non-NULL and NULL values in all columns of a specified Oracle table.
    Uses configuration from .env file.
    """
    try:
        #######################################
        ###### Configure it as you need #######
        #######################################
        table_name = 'CORP_PARTY'
        schema_name = 'COLIN_MGR_TST' # this is tst, you can use dev too COLIN_MGR_DEV
        row_limit = 10000 # there are a loooot of rows in some tables, pick your number
        #######################################
        
        if not schema_name or not table_name:
            raise ValueError("Missing schema configuration or table name")
        
        # Create database connection
        print("Creating db connection")
        connection_string = get_connection_string()
        engine = create_engine(connection_string)
        print("db connected")
        print('-'*100)
        
        # Get total row count first
        with engine.connect() as conn:
            total_table_rows = pd.read_sql(
                f"SELECT COUNT(*) as count FROM {schema_name}.{table_name}",
                conn
            ).iloc[0]['count']
        
        # Read limited data from the table
        print(f"Reading random samples from {schema_name}.{table_name} (sample size: {row_limit:,} rows)...")
        query = f"""
            SELECT * FROM (
                SELECT * FROM {schema_name}.{table_name}
                ORDER BY DBMS_RANDOM.VALUE
            ) WHERE ROWNUM <= {row_limit}
        """
        df = pd.read_sql(query, engine)
        
        # Get sample size
        sample_rows = len(df)
        print(f"\nAnalyzing sample of {sample_rows:,} rows out of {total_table_rows:,} total rows")
        print(f"Sample represents {(sample_rows/total_table_rows)*100:.2f}% of the table")
        print('-'*100)
        
        # Calculate non-NULL counts and percentages for each column
        results = []
        for column in df.columns:
            non_null_count = df[column].count()
            non_null_percentage = (non_null_count / sample_rows) * 100
            
            results.append({
                'Column Name': column,
                'Non-NULL Count': non_null_count,
                'Non-NULL Percentage': f"{non_null_percentage:.2f}%",
            })
        
        # Convert results to DataFrame and sort by non-NULL count in descending order
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('Non-NULL Count', ascending=False)
        
        return results_df
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Configure pandas display options
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.float_format', lambda x: '%.2f' % x)
        
        # Run analysis
        results = analyze_table_nulls()
        
        # Print results
        print("\nAnalysis Results:")
        print("-" * 100)
        print(results)
        
    except Exception as e:
        print(f"Failed to complete analysis: {str(e)}")
