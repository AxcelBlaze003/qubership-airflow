import os
import psycopg2
import logging

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] [%(filename)s] [thread=%(threadName)s] %(message)s",
    level=logging.DEBUG,
)

postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
postgres_admin_user = os.getenv("POSTGRES_ADMIN_USER")
postgres_admin_password = os.getenv("POSTGRES_ADMIN_PASSWORD")

check_if_db_exists = (
    f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}';"
)
create_db = f"CREATE DATABASE {db_name};"
check_if_user_exists = f"SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{db_user}';"
create_user = f"CREATE USER {db_user} PASSWORD '{db_password}';"
grant_permissions = f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user};"
grant_ownership = f"ALTER DATABASE {db_name} OWNER TO {db_user};"

conn = psycopg2.connect(
    database="postgres",
    user=postgres_admin_user,
    password=postgres_admin_password,
    host=postgres_host,
    port=postgres_port,
)

conn.autocommit = True
cursor = conn.cursor()


cursor.execute(check_if_db_exists)
exists = cursor.fetchone()
if not exists:
    logging.info("db doesn't exist, creating ...")
    cursor.execute(create_db)
    logging.info("db created ...")

cursor.execute(check_if_user_exists)
exists = cursor.fetchone()
if not exists:
    logging.info("user doesn't exist, creating ...")
    cursor.execute(create_user)
    logging.info("user created ...")

cursor.execute(grant_permissions)
cursor.execute(grant_ownership)
logging.info("permissions were configured")

cursor.close()
conn.close()
