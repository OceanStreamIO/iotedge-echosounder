import sqlite3
import datetime

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
                filename_processed TEXT NOT NULL,
                filename_raw TEXT NOT NULL,
                file_start_date TEXT NOT NULL,
                file_end_date TEXT NOT NULL,
                processed_date TEXT NOT NULL,
                additional_info TEXT
            )
        ''')
        self.connection.commit()

    def file_processed_before(self, filename):
        """Check if a file has been processed before."""
        self.cursor.execute('''
            SELECT 1 FROM processed_files WHERE filename_processed = ?
        ''', (filename,))
        
        return bool(self.cursor.fetchone())

    def mark_file_as_processed(self, filename, additional_info=None):
        """Mark a file as processed by adding it to the database."""
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        self.cursor.execute('''
            INSERT INTO processed_files (filename_processed, filename_raw,file_start_date,file_end_date, processed_date, additional_info) VALUES (?, ?, ?, ?, ?, ?)
        ''', (filename,filename, now, now, now, additional_info))
        
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