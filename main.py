import pyodbc
import pymysql
#pip install pymysql
#pip install pyodbc


def connect_to_sql_server():
    conn_str = (
        "DRIVER={SQL Server};"
        "SERVER=localhost\\sqlexpress;"
        "DATABASE=databaseName;"  
        "UID=sa;"
        "PWD=pass;"
    )
    return pyodbc.connect(conn_str)


def connect_to_mysql():
    return pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="pass",
        database="dbo",
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def copy_data_from_sql_to_mysql(sql_table, mysql_table):
    try:
        sql_conn = connect_to_sql_server()
        mysql_conn = connect_to_mysql()

        sql_cursor = sql_conn.cursor()
        sql_cursor.execute(f"SELECT * FROM {sql_table}")
        rows = sql_cursor.fetchall()

        columns = [column[0] for column in sql_cursor.description]
        column_names = ', '.join(columns)

        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {mysql_table} ({column_names}) VALUES ({placeholders})"

        mysql_cursor = mysql_conn.cursor()

        try:
            mysql_cursor.execute(f"TRUNCATE TABLE {mysql_table};")

            batch_size = 400
            row_count = 0

            for row in rows:
                row_values = list(row)
                mysql_cursor.execute(insert_query, row_values)
                row_count += 1

                if row_count % batch_size == 0:
                    mysql_conn.commit()
                    print(f"Zatwierdzono {row_count} wierszy do {mysql_table}")

            mysql_conn.commit()
            print(f"Przeniesiono łącznie {row_count} wierszy z {sql_table} do {mysql_table}")

        except Exception as insert_error:
            print(f"Wystąpił błąd podczas wstawiania danych: {insert_error}")
            mysql_conn.rollback()  # Wycofanie zmian w przypadku błędu

    except Exception as e:
        print(f"Wystąpił błąd: {e}")

    finally:
        # Zamknięcie połączeń
        if sql_cursor:
            sql_cursor.close()
        if sql_conn:
            sql_conn.close()
        if mysql_cursor:
            mysql_cursor.close()
        if mysql_conn:
            mysql_conn.close()


def get_tables_from_mssql():
    sql_conn = connect_to_sql_server()
    sql_cursor = sql_conn.cursor()
    sql_cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    tables = sql_cursor.fetchall()
    sql_cursor.close()
    sql_conn.close()
    return [table[0] for table in tables]


if __name__ == "__main__":
    tables = get_tables_from_mssql()
    for table in tables:
        sql_table = table
        mysql_table = table
        copy_data_from_sql_to_mysql(sql_table, mysql_table)

'''
if __name__ == "__main__":
    sql_table = "pliki"  # Zmień na nazwę swojej tabeli
    mysql_table = "pliki"  # Zmień na nazwę tabeli docelowej w MySQL
    copy_data_from_sql_to_mysql(sql_table, mysql_table)
'''
