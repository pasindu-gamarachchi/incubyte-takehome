import os
import logging
import sqlite3


class HospitalDatabaseSystem:
    accepted_countries = ["Table_India"]

    def __init__(self, expected_extension):
        self.expected_extension = expected_extension
        self.conn = None
        self.cursor = None

    def connect_to_db(self):
        """
        Connect to Postgres
        :return:
        """
        try:
            self.conn = sqlite3.connect(':memory:')  # Create DB in memory
            self.cursor = self.conn.cursor()
        except Exception as err:
            logging.info(err)

    def check_accepted_country(self, country):
        """
        Check if the country is in accepted coutries.
        To prevent SQL injection Attacks.
        :param country:
        :return:
        """
        if country not in self.accepted_countries:
            raise Exception("Input Country not allowed. For : " + country)

    def create_table(self, country):
        """
        Create Table
        :return:
        """
        country = country.strip()
        self.check_accepted_country(country)
        # if country not in self.accepted_countries:
        #    raise Exception ("Input Country not allowed.")

        create_table_str = """CREATE TABLE IF NOT EXISTS {} 
                            (Name VARCHAR(255) NOT NULL PRIMARY KEY,
                             Cust_I VARCHAR(18) NOT NULL,
                             Open_Dt DATE NOT NULL,
                             Consul_Dt DATE,
                             VAC_ID CHAR(5),
                             DR_Name VARCHAR(255), 
                             State CHAR(5), 
                             Country CHAR(5),
                             DOB DATE,
                             FLAG CHAR(1)
                             )""".format(country)
        self.cursor.execute(create_table_str)

    def read_data_from_file(self, filename):
        """
        Read data from a text File.
        :return:
        """
        if os.path.splitext(filename)[-1] != self.expected_extension:
            raise Exception("Unexpected File type. Expecting -> " + self.expected_extension)

        self.read_txt_file(filename)

    def read_txt_file(self, filename):
        """
        Read data from a text file.
        :param filename: filename to read data from.
        :return:
        """
        with open(filename) as f:
            for ind, line in enumerate(f):
                if ind > 0:
                    self.write_to_db(line)

    def write_to_db(self, line):
        """
        Write data to DB
        :param line: string of data to be inserted
        :return:
        """
        data_to_insert = [val.strip() for val in line.split('|')[1:]]
        if len(data_to_insert) == 0:
            raise Exception("Missing Data.")

        if len(data_to_insert) > 0:
            cleaned_data_to_insert = [val if val != "" else None for val in data_to_insert[1:]]
            self.cursor.execute("INSERT INTO Table_India VALUES (?,?,?,?,?,?,?,?,?,?)",
                                (*cleaned_data_to_insert,))

        # if len()
        # print(data_to_insert[1:])


if __name__ == '__main__':
    hdb = HospitalDatabaseSystem('.txt')
    hdb.connect_to_db()
    hdb.create_table('Table_India')
    hdb.read_data_from_file('sample_data_file.txt')
    hdb.cursor.execute("""SELECT * FROM Table_India""")
    [print(doc) for doc in hdb.cursor.fetchall()]
