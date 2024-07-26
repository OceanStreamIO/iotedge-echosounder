import sqlite3
import datetime
from pathlib import Path

DB_PATH = "processed_files.db"


class DBHandler:
    def __init__(self):
        self.connection = sqlite3.connect(DB_PATH)
        self.cursor = self.connection.cursor()

    def setup_database(self):
        """Create tables if they don't already exist."""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY,
                filename_processed TEXT,
                filetype_processed TEXT,
                filename_raw TEXT NOT NULL,
                filetype_raw TEXT NOT NULL,
                file_start_date TIMESTAMP,
                file_end_date TIMESTAMP,
                processed_start_date TIMESTAMP NOT NULL,
                processed_end_date TIMESTAMP NOT NULL,
                additional_info TEXT
            )
        ''')
        self.connection.commit()

    def file_processed_before(self, filename):
        """Check if a file has been processed before."""
        self.cursor.execute('''
            SELECT 1 FROM processed_files WHERE filename_raw = ?
        ''', (filename,))

        return bool(self.cursor.fetchone())

    def mark_file_as_processed(self,
                               filename_raw,
                               filename_processed=None,
                               start_date=None,
                               end_date=None,
                               start_processing=None,
                               additional_info=None):
        """Mark a file as processed by adding it to the database."""
        end_processing = datetime.datetime.now()
        if not start_processing:
            start_processing = end_processing
        insert_query = """INSERT INTO processed_files (filename_processed,
                                                       filetype_processed,
                                                       filename_raw,
                                                       filetype_raw,
                                                       file_start_date,
                                                       file_end_date,
                                                       processed_start_date,
                                                       processed_end_date,
                                                       additional_info)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        insert_values = (filename_processed,
                         None if filename_processed is None else Path(filename_processed).suffix,
                         filename_raw,
                         Path(filename_raw).suffix,
                         start_date,
                         end_date,
                         start_processing,
                         end_processing,
                         additional_info)

        self.cursor.execute(insert_query, insert_values)

        self.connection.commit()

    def close(self):
        """Close the database connection."""
        self.connection.close()

# Example usage:
# db = DBHandler()
# db.setup_database()
# if not db.file_processed_before('example.txt'):
#     db.mark_file_as_processed('example.txt', "Processed without errors")
# db.close()
