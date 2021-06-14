import unittest
import sqlite3
from Hospital_Database_System import HospitalDatabaseSystem


class TestHospitalDatabaseSystem(unittest.TestCase):

    expected_col_names = {
        'VAC_ID', 'Country', 'Cust_I', 'Open_Dt', 'DOB', 'DR_Name', 'State', 'Consul_Dt', 'Name', 'FLAG'
    }

    @classmethod
    def setUpClass(cls) -> None:
        cls._HDS = HospitalDatabaseSystem('.txt')
        cls._HDS.connect_to_db()

    def test_connect_to_db(self):
        print("Testing connect_to_db")
        self._HDS.connect_to_db()
        # self.assertIsNotNone(self._HDS.conn)
        self.assertIsInstance(self._HDS.conn, sqlite3.Connection)
        self.assertIsInstance(self._HDS.cursor, sqlite3.Cursor)

    def test_check_accepted_country(self):
        print("Testing check_accepted_country")
        with self.assertRaises(Exception):
            self._HDS.check_accepted_country('India')

    def test_create_table(self):
        print("Testing create_table")

        table_name = "Table_India"
        self._HDS.create_table(table_name)
        table_exists_str = """
            SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name;
            """
        self._HDS.cursor.execute(table_exists_str, {'table_name': table_name})
        table_res = self._HDS.cursor.fetchone()
        self.assertEqual(table_res[0], table_name)
        table_col_names = self.get_table_col_names(table_name)
        self.assertEqual(table_col_names, self.expected_col_names)

    def get_table_col_names(self, table_name):
        table_cols_str = f"PRAGMA table_info ('{table_name}')"
        table_cols_res = self._HDS.cursor.execute(table_cols_str)
        table_cols_res = self._HDS.cursor.fetchall()
        # print(f"Table col res -> {table_cols_res}")
        table_cols = set()
        for col in table_cols_res:
            table_cols.add(col[1])

        return table_cols

    def test_read_data_from_file(self):
        print("Testing read_data_from_file.")
        with self.assertRaises(Exception):
            self._HDS.read_data_from_file('sample_data_file.json')

        #with self.assertRaises(Exception):
        self._HDS.read_data_from_file('sample_data_file.txt')

    def test_read_txt_file(self):
        pass

    def test_write_to_db(self):
        print("Testing write_to_db.")

        with self.assertRaises(Exception):
            self._HDS.write_to_db('')
        # self._HDS.write_to_db(['', '1256', '20101012', '20121013', 'MVD', 'Paul', 'VIC', 'AU', '06031987', 'A'])
        with self.assertRaises(sqlite3.IntegrityError):
            print("Testing Missing Name")
            self._HDS.write_to_db('|D||123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A')
        with self.assertRaises(sqlite3.IntegrityError):
            print("Testing Missing Cust ID")
            self._HDS.write_to_db('|D|John||20101012|20121013|MVD|Paul|TN|IND|06031987|A')
        with self.assertRaises(sqlite3.IntegrityError):
            print("Testing Missing Cust Open Date")
            self._HDS.write_to_db('|D|Mathew|123459||20121013|MVD|Paul|WAS|PHIL|06031987|A')

        with self.assertRaises(sqlite3.IntegrityError):
            print("Testing Primary Key")
            self._HDS.write_to_db('|D|Alex|123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A')
            self._HDS.write_to_db('|D|Alex|123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A')

        # self._HDS.write_to_db('|D|Alex|123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A')
        print("Test sample insertion.")
        self._HDS.cursor.execute("""SELECT * FROM Table_India WHERE name='Alex'""")
        alex_data = self._HDS.cursor.fetchone()
        expected_alex_data = ('Alex', '123457', 20101012, 20121013, 'MVD', 'Paul', 'SA', 'USA', 6031987, 'A')
        self.assertEqual(expected_alex_data, alex_data)

        # Test minimum input
        self._HDS.write_to_db('|D|min_input_name|123457|20101012|||||||')
        self._HDS.cursor.execute("""SELECT * FROM Table_India WHERE name='min_input_name'""")
        min_input_name_data = self._HDS.cursor.fetchone()
        expected_min_input_name_data = ('min_input_name', '123457', 20101012, None, None, None, None, None, None, None)
        self.assertEqual(expected_min_input_name_data, min_input_name_data)

        # Data Types not tested as SQL Lite allows dyanmic data types.


if __name__ == '__main__':
    unittest.main()
