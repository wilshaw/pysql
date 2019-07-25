import pyodbc
import subprocess
import socket
import uuid


def get_tmp_share():
    hostname = socket.gethostname()
    return f'\\\\{hostname}\\c$\\tmp'


def get_sql_driver():
    return "SQL Server Native Client 11.0"


def bcp_export(server, database, query, bcp_filename):
    bcp_command = f"bcp \"{query}\" queryout {bcp_filename} -d {database} -c -T -S {server}"
    print(f'Running command: {bcp_command}')
    p = subprocess.Popen(bcp_command)
    p.wait()


def get_sql_type(column_type):
    if column_type == "int":
        return "int"
    if column_type == "datetime":
        return "datetime"
    if column_type == "float":
        return "real"
    raise NotImplementedError


def execute_no_query(server, database, sql):
    conn = pyodbc.connect(f"Driver={get_sql_driver()};"
                          f"Server={server};"
                          f"Database={database};"
                          "Trusted_Connection=yes;")
    cursor = conn.cursor()
    print(f"Executing query: {sql}")
    cursor.execute(sql)
    cursor.commit()
    conn.commit()


def get_table_columns(server, database, table):
    conn = pyodbc.connect(f"Driver={get_sql_driver()};"
                          f"Server={server};"
                          f"Database={database};"
                          "Trusted_Connection=yes;")
    cursor = conn.cursor()
    cursor.execute(f'SELECT TOP 0 * FROM {table}')
    columns = cursor.description
    cursor.commit()
    conn.commit()

    return columns


def get_table_primary_key_column_ordinals(server, database, table):
    conn = pyodbc.connect(f"Driver={get_sql_driver()};"
                          f"Server={server};"
                          f"Database={database};"
                          "Trusted_Connection=yes;")
    cursor = conn.cursor()
    cursor.execute(f'SELECT TOP 0 * FROM {table}')

    primary_key_columns = []
    for row in cursor.primaryKeys(table):
        key_seq = row[4]
        primary_key_columns.append(key_seq)

    cursor.commit()
    conn.commit()

    return primary_key_columns


def get_table_primary_key_columns(server, database, table, columns):
    primary_key_column_ordinals = get_table_primary_key_column_ordinals(server, database, table)
    primary_key_columns = []
    for ordinal in primary_key_column_ordinals:
        primary_key_columns.append(columns[ordinal - 1]) # SQL ordinals are 1 based
    return primary_key_columns


def create_table_if_not_exists(server, database, table, columns):
    column_definitions = []
    for c in columns:
        column_name = c[0]
        column_type = c[1].__name__
        sql_column_type = get_sql_type(column_type)
        if c[6]:
            column_null = 'NULL'
        else:
            column_null = 'NOT NULL'
        column_definitions.append(f"{column_name} {sql_column_type} {column_null}")
        columns_sql = ',\n'.join(column_definitions)
    sql = f"""
IF OBJECT_ID('dbo.{table}') IS NULL
BEGIN
CREATE TABLE dbo.{table}
(
{columns_sql}
)
END
    """
    execute_no_query(server, database, sql)


def bulk_insert_file(server, database, table, bcp_filename):
    sql = f"""
    BULK INSERT {table}
    FROM '{bcp_filename}'
    WITH (TABLOCK)
    """
    execute_no_query(server, database, sql)


def truncate_table(server, database, table):
    sql = f"""
        TRUNCATE TABLE {table}
        """
    execute_no_query(server, database, sql)


def upsert_data(server, database, table, staging_table, columns, primary_key_columns):
    conn = pyodbc.connect(f"Driver={get_sql_driver()};"
                          f"Server={server};"
                          f"Database={database};"
                          "Trusted_Connection=yes;")
    cursor = conn.cursor()

    column_names = []
    for c in columns:
        column_names.append(c[0])
    columns_sql = ', '.join(column_names)

    primary_key_sql = "WHERE "
    i = 0
    for c in primary_key_columns:
        primary_key_sql += f"s.{c[0]} = d.{c[0]}"
        i += 1
        if i < len(primary_key_columns):
            primary_key_sql += " AND "

    sql = f"""
    INSERT INTO {database}.dbo.{table} ({columns_sql})
    SELECT id, date, other, value
    FROM {database}.dbo.{staging_table} s
    WHERE NOT EXISTS (SELECT 1 FROM {database}.dbo.{table} d {primary_key_sql})
    """
    cursor.execute(sql)
    # for row in cursor:
    #     print('row = %r' % (row,))

    cursor.commit()
    conn.commit()


def synchronise_db_table(source_server, source_database, source_table, destination_server,
                         destination_database, destination_table, clause, bcp_location):
    # Export data to bcp file
    columns = get_table_columns(source_server, source_database, source_table)
    primary_key_columns = get_table_primary_key_columns(source_server, source_database, source_table, columns)
    source_query = f"SELECT {', '.join([c[0] for c in columns])} FROM {source_database}.dbo.{source_table} {clause}"
    bcp_filename = f'{bcp_location}\\{str(uuid.uuid4())}.dat'
    bcp_export(source_server, source_database, f"{source_query}", bcp_filename)

    # Insert into staging table
    staging_table = f'{destination_table}_Staging'
    create_table_if_not_exists(destination_server, destination_database, staging_table, columns)
    bulk_insert_file(destination_server, destination_database, staging_table, bcp_filename)

    # Upsert into destination table
    upsert_data(destination_server, destination_database, destination_table, staging_table, columns, primary_key_columns)

    # Truncate staging table
    truncate_table(destination_server, destination_database, staging_table)

    print('Done')


def main():
    bcp_location = get_tmp_share()
    source_server = "DESKTOP-6G5P2TO\\MSSQLSERVER01"
    source_database = "PySQL"
    source_table = "TestSource"
    destination_server = "DESKTOP-6G5P2TO\\MSSQLSERVER01"
    destination_database = "PySQLCopy"
    destination_table = "TestDestination"
    clause = "WHERE id != 1000"
    synchronise_db_table(source_server, source_database, source_table, destination_server, destination_database,
                         destination_table, clause, bcp_location)


main()
