import os
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=DictCursor)

def get_db():
    is_testing = os.getenv("TESTING", "false").lower() == "true"
    conn = None
    try:
        conn = get_db_connection()
        yield conn
    finally:
        if conn and not is_testing:
            conn.close()