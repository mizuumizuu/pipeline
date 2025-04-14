SQL_SCRIPTS_PATH='/opt/airflow/dags/sql'

DB_CONFIG = {
    "dbname": "postgres_db",
    "user": "postgres_user",
    "password": "postgres_password",
    "host": "de-postgres",
    "port": "5432"
}

DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"