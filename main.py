import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
load_dotenv(dotenv_path='/path/to/.env')

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

EXCEL_FILE_PATH = "../AgeDetectionFraud/Synthetic_Financial_datasets_log_test.csv"

data = pd.read_csv(EXCEL_FILE_PATH)
print("Current working directory:", os.getcwd())

print("Columns", data.columns)


if 'is_fraud' in data.columns:
    data['is_fraud'] = data['is_fraud'].astype(bool)
else:
    print("'is_fraud' column not found Skipping 'is_fraud' transformation.")

def create_database():
    try:
        conn = psycopg2.connect(
            dbname='postgres', user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute(f"CREATE DATABASE {DB_NAME};")
        print(f"Database '{DB_NAME}' created successfully.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")

def create_age_extension():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        cursor = conn.cursor()

        cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")
        cursor.execute("SET search_path = ag_catalog, '$user', public;")
        print("AGE extension created and search path set successfully.")

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating AGE extension: {e}")
create_database()
create_age_extension()


def create_table():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id SERIAL PRIMARY KEY,
            step INTEGER,
            type VARCHAR(50),
            amount DECIMAL,
            name_orig VARCHAR(50),
            oldbalance_org DECIMAL,
            newbalance_orig DECIMAL,
            name_dest VARCHAR(50),
            oldbalance_dest DECIMAL,
            newbalance_dest DECIMAL,
            is_fraud BOOLEAN,
            is_flagged_fraud BOOLEAN
        );
        """
        cursor.execute(create_table_query)
        print("Transactions table created successfully with all required columns.")

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating table: {e}")

create_table()


def load_data_to_postgres():
    try:
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        data.to_sql('transactions', engine, if_exists='append', index=False)
        print("Data loaded into PostgreSQL successfully.")
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")

load_data_to_postgres()

# Step 6: Model Data as Graph in AGE
# def create_graph_model():
#     try:
#         conn = psycopg2.connect(
#             dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
#         )
#         cursor = conn.cursor()
#
#         # Create a graph with accounts and transactions as nodes and relationships
#         graph_creation_query = """
#         SELECT * FROM cypher('transactions', $$
#             CREATE (:Account {name: 'A', balance: 5000})-[:TRANSFER {amount: 1000}]->(:Account {name: 'B', balance: 7000})
#         $$) AS (v agtype);
#         """
#         cursor.execute(graph_creation_query)
#         print("Graph model created successfully in AGE.")
#
#         conn.commit()
#         cursor.close()
#         conn.close()
#     except Exception as e:
#         print(f"Error creating graph model in AGE: {e}")
#
# create_graph_model()
#
# # Step 7: Sample Cypher Query for Fraud Detection
# def run_fraud_detection_query():
#     try:
#         conn = psycopg2.connect(
#             dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
#         )
#         cursor = conn.cursor()
#
#         # Example Cypher query to identify suspicious transactions
#         query = """
#         SELECT * FROM cypher('transactions', $$
#             MATCH (a:Account)-[t:TRANSFER]->(b:Account)
#             WHERE t.amount > 10000 AND a.balance < t.amount
#             RETURN a, t, b
#         $$) AS (a agtype, t agtype, b agtype);
#         """
#         cursor.execute(query)
#         result = cursor.fetchall()
#         print("Fraud detection query results:")
#         for row in result:
#             print(row)
#
#         conn.commit()
#         cursor.close()
#         conn.close()
#     except Exception as e:
#         print(f"Error running fraud detection query: {e}")
#
# run_fraud_detection_query()