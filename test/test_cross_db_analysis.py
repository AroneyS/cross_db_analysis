import unittest, tempfile, os.path, sys

path_to_data = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data')

sys.path = [os.path.join(os.path.dirname(os.path.realpath(__file__)),'..')]+sys.path
from cross_db_analysis import SqliteDatabase
from cross_db_analysis import CrossDatabaseComparator



class TestSqliteDatabase(unittest.TestCase):
    def test_initialise_database(self):
        with tempfile.NamedTemporaryFile(mode='w') as db_path:
            db = SqliteDatabase(db_path.name)
            db.execute("CREATE TABLE test (id integer PRIMARY KEY, name text);")
            db.execute("INSERT INTO test(name) VALUES ('test value');")
            observed = db.execute("SELECT * FROM test;").fetchall()
            
            self.assertEqual(observed, [(1, 'test value')])


class TestCrossDatabaseComparator(unittest.TestCase):
    reads_db_path = os.path.join(path_to_data, 'reads_combined.sdb', 'otus.sqlite3')
    assemblies_db_path = os.path.join(path_to_data, 'assemblies_combined.sdb', 'otus.sqlite3')
    bins_db_path = os.path.join(path_to_data, 'bins_combined.sdb', 'otus.sqlite3')

    def CreateComparator(self, reads_db_path = None, assemblies_db_path = None,
                         bins_db_path = None, output_db_path = None, output_path = None):
        if reads_db_path is None: reads_db_path = self.reads_db_path
        if assemblies_db_path is None: assemblies_db_path = False
        if bins_db_path is None: bins_db_path = self.bins_db_path
        if output_db_path is None: output_db_path = False
        if output_path is None: output_path = tempfile.NamedTemporaryFile(mode='w')

        comparator = CrossDatabaseComparator(
            reads_db=reads_db_path,
            assemblies_db=assemblies_db_path,
            bins_db=bins_db_path,
            output_db=output_db_path,
            output=output_path)
        return comparator


    def test_creates_output_database(self):
        output_db_file = tempfile.NamedTemporaryFile(mode='w')
        comparator = self.CreateComparator(output_db_path=output_db_file.name)
        self.assertIsInstance(comparator.output_db, SqliteDatabase)
    
    def test_creates_tmp_output_database(self):
        comparator = self.CreateComparator()
        self.assertIsInstance(comparator.output_db, SqliteDatabase)
            

if __name__ == "__main__":
    unittest.main()
