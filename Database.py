from DbConnector import DbConnector
from tabulate import tabulate
from typing import Optional
import logging

class Database:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self) -> bool:
        try:
            query = """
                    CREATE TABLE IF NOT EXISTS User(
                        id CHAR(3) NOT NULL,
                        has_labels BOOL NOT NULL,
                        PRIMARY KEY(id)
                    );
                    """
            self.cursor.execute(query)
            self.db_connection.commit()
        except Exception as e:
            logging.critical(f"An error occured in create_user_table():\n{e}")
            return False
        return True
    
    def create_activity_table(self) -> bool:
        try:
            query = """
                    CREATE TABLE IF NOT EXISTS Activity (
                        id INT NOT NULL,
                        user_id CHAR(3) NOT NULL,
                        transportation_mode VARCHAR(16),
                        start_date_time DATETIME NOT NULL,
                        end_date_time DATETIME NOT NULL,
                        PRIMARY KEY(id),
                        FOREIGN KEY(user_id) REFERENCES User(id)
                    );
                    """
            self.cursor.execute(query)
            self.db_connection.commit()
        except Exception as e:
            logging.critical(f"An error occured in create_activity_table():\n{e}")
            return False
        return True
    
    def create_trackpoint_table(self) -> bool:
        try:
            query = """
                    CREATE TABLE IF NOT EXISTS TrackPoint (
                        id INT NOT NULL AUTO_INCREMENT,
                        activity_id INT NOT NULL,
                        lat DOUBLE NOT NULL,
                        lon DOUBLE NOT NULL,
                        altitude INT NOT NULL,
                        date_days DOUBLE NOT NULL,
                        date_time DATETIME NOT NULL,
                        PRIMARY KEY(id),
                        FOREIGN KEY(activity_id) REFERENCES Activity(id)
                    );
                    """
            self.cursor.execute(query)
            self.db_connection.commit()
        except Exception as e:
            logging.critical(f"An error occured in create_trackpoint_table():\n{e}")
            return False
        return True

    def insert_user(self, id : str, has_labels : bool) -> bool:
        try:
            query = "INSERT INTO User (id, has_labels) VALUES (%s, %s);"
            self.cursor.execute(query, (id, has_labels))
            self.db_connection.commit()
        except Exception as e:
            logging.critical(f"An error occured in insert_user():\n{e}")
            return False
        return True
    
    def insert_activity(self,
                        id : int,
                        user_id : str, 
                        start_date_time : str,
                        end_date_time : str, 
                        transportation_mode : Optional[str] = None) -> bool:
        try:
            query = "INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s);"
            self.cursor.execute(query, (id, 
                                        user_id, 
                                        transportation_mode, 
                                        start_date_time, 
                                        end_date_time))
            self.db_connection.commit()
        except Exception as e:
            logging.critical(f"An error occured in insert_activity():\n{e}")
            return False
        return True

    def insert_trackpoints(self, data : list[tuple[int, float, float, int, float, str]]) -> bool:
        try:
            query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s);"
            self.cursor.executemany(query, data)
            self.db_connection.commit()
        except Exception as e:
            logging.critical(f"An error occured in insert_trackpoints():\n{e}")
            return False
        return True

    def fetch_data(self, table_name) -> list[tuple]:
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows
    
    def query_data(self, query : str) -> list[tuple]:
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
        except Exception as e:
            logging.warning(f"An error occured in query_data():\n{e}")
            return
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))