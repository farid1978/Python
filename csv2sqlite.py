import sys
import csv
import sqlite3
import os

def csv_to_sqlite(csv_file, db_name, table_name):
    try:
        # Connect to SQLite database (or create it if it doesn't exist)
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Open the CSV file with utf-8 encoding and ignore errors
        with open(csv_file, mode='r', encoding='utf-8', errors='ignore') as csvfile:
            reader = csv.reader(csvfile)

            # Extract header and create table
            headers = next(reader)
            print("Headers:", headers)
            columns = ', '.join(['"{}" TEXT'.format(header) for header in headers])

            create_table_query = 'CREATE TABLE IF NOT EXISTS "{}" ({});'.format(table_name, columns)
            print("Create Table Query:", create_table_query)
            cursor.execute(create_table_query)

            # Insert the data
            for row in reader:
                # Process each cell to handle non-ASCII characters properly
                row = [cell.encode('utf-8', 'ignore').decode('utf-8') for cell in row]
                placeholders = ', '.join(['?' for _ in row])
                insert_query = 'INSERT INTO "{}" VALUES ({});'.format(table_name, placeholders)
                print("Insert Query:", insert_query, row)
                cursor.execute(insert_query, row)

            # Commit changes and close connection
            conn.commit()
            print("Data from '{}' has been successfully imported into the SQLite database '{}' in table '{}'.".format(csv_file, db_name, table_name))

    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: csv_to_sqlite.py <filename.csv>")
        sys.exit(1)

    csv_file = str(sys.argv[1])
    filename, ext = os.path.splitext(os.path.basename(csv_file))
    name = filename.split('.')[0]

    db_name = "ucs.db"
    print("Filename:", name)
    table_name = name

    csv_to_sqlite(csv_file, db_name, table_name)