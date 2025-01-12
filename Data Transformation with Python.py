import psycopg2
import pandas as pd

hostname = 'localhost'
database = 'Company_Set'
username = 'postgres'
pwd = '250857'
port_id = 5432
conn = None
cur = None

try:

    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()


    file_path = r"C:\Users\davpt\OneDrive\Desktop\Akshat\question1_large_user_data.csv"
    df = pd.read_csv(file_path)
    output_file_path = r"C:\Users\davpt\OneDrive\Desktop\Akshat\titanic.csv"

    cutoff_date = "2023-01-01"
    
    df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')
    filtered_df = df[df['signup_date'] >= pd.Timestamp(cutoff_date)]
    filtered_df.to_csv(output_file_path, index=False)
    print(f"Filtered data saved to: {output_file_path}")

    table_name = "q1_csv"
    create_script = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        user_id BIGINT,
        name TEXT,
        email TEXT,
        signup_date DATE
    );
    """
    
    cur.execute(create_script)

    with open(file_path, 'r') as file:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", file)

    conn.commit()

except Exception as error:
    print("Error:", error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
