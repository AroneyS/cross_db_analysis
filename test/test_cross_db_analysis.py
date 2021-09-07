import unittest, tempfile, os.path, sys, sqlite3

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
    reads_db_first = [(1, 'novaseq.20110700_P3D.1', 1, 1.641304347826087, 'Root; d__Eukaryota; Fungi', 1, 1)]
    assemblies_db_path = os.path.join(path_to_data, 'assemblies_combined.sdb', 'otus.sqlite3')
    assemblies_db_first = [(1, 'Fen.Labelled.R1.T3.F1.trimmomatic.megahit.final.contigs', 1, 1.1878980891719746, 'Root; d__Eukaryota; Amoebozoa', 1, 1)]
    bins_db_path = os.path.join(path_to_data, 'bins_combined.sdb', 'otus.sqlite3')
    bins_db_first = [(1, 'Bog.None.R2.T0.trimmomatic.megahit_bin.272_80.28_7.13', 1, 1.007074340527578, 'Root; d__Bacteria; p__Desulfobacterota_B; c__Binatia; o__UTPRO1; f__UTPRO1; g__UTPRO1; s__UTPRO1_sp002050235', 1, 1)]

    def CreateComparator(self, reads_db_path = None, assemblies_db_path = None,
                         bins_db_path = None, output_db_path = None, output_path = None, force = None):
        if reads_db_path is None: reads_db_path = self.reads_db_path
        if assemblies_db_path is None: assemblies_db_path = False
        if bins_db_path is None: bins_db_path = self.bins_db_path
        if output_db_path is None: output_db_path = False
        if output_path is None: output_path = tempfile.NamedTemporaryFile(mode='w')
        if force is None: force = False

        comparator = CrossDatabaseComparator(
            reads_db=reads_db_path,
            assemblies_db=assemblies_db_path,
            bins_db=bins_db_path,
            output_db=output_db_path,
            output=output_path,
            force=force)
        return comparator


    def test_creates_output_database(self):
        with tempfile.NamedTemporaryFile(mode='w') as output_db_file:
            comparator = self.CreateComparator(output_db_path=output_db_file.name)
            self.assertIsInstance(comparator.output_db, SqliteDatabase)
    
    def test_creates_tmp_output_database(self):
        comparator = self.CreateComparator()
        self.assertIsInstance(comparator.output_db, SqliteDatabase)
 
    def test_overwrites_output_database(self):
        with tempfile.NamedTemporaryFile(mode='w') as output_db_file:
            db = SqliteDatabase(output_db_file.name)
            create_cmd = "CREATE TABLE IF NOT EXISTS test (id integer PRIMARY KEY, name text);"
            db.execute(create_cmd)
            db.execute("INSERT INTO test(name) VALUES ('test value');")
            db.execute("COMMIT;")
            del(db)

            comparator = self.CreateComparator(output_db_path=output_db_file.name, force=True)
            comparator.output_db.execute(create_cmd)
            observed = comparator.output_db.execute("SELECT * FROM test;").fetchall()

            expected = []
            self.assertEqual(observed, expected)

    
    def assertAttachedDatabase(self, main_db, attach_db, attach_table, expected):
        cmd = f"SELECT * FROM {attach_db}.{attach_table} LIMIT 1;"
        observed = main_db.execute(cmd).fetchall()
        self.assertEqual(observed, expected)
    
    def test_input_databases_attached(self):
        comparator = self.CreateComparator(assemblies_db_path=self.assemblies_db_path)
        self.assertAttachedDatabase(comparator.output_db, comparator.reads_db_name, "otus", self.reads_db_first)
        self.assertAttachedDatabase(comparator.output_db, comparator.assemblies_db_name, "otus", self.assemblies_db_first)
        self.assertAttachedDatabase(comparator.output_db, comparator.bins_db_name, "otus", self.bins_db_first)

    def test_assemblies_database_not_attached(self):
        comparator = self.CreateComparator()
        with self.assertRaises(sqlite3.OperationalError) as context:
            self.assertAttachedDatabase(comparator.output_db, comparator.assemblies_db_name, "otus", self.assemblies_db_first)

    def test_input_sdb_folder_attaches(self):
        comparator = self.CreateComparator(
            reads_db_path=os.path.join(path_to_data, 'reads_combined.sdb'),
            assemblies_db_path=os.path.join(path_to_data, 'assemblies_combined.sdb'),
            bins_db_path=os.path.join(path_to_data, 'bins_combined.sdb')
        )
        self.assertAttachedDatabase(comparator.output_db, comparator.reads_db_name, "otus", self.reads_db_first)
        self.assertAttachedDatabase(comparator.output_db, comparator.assemblies_db_name, "otus", self.assemblies_db_first)
        self.assertAttachedDatabase(comparator.output_db, comparator.bins_db_name, "otus", self.bins_db_first)
    

    def test_create_compare_table(self):
        comparator = self.CreateComparator()
        comparator.compare()
        
        cmd = f"SELECT * FROM {comparator.compare_table_name} WHERE bin=0 LIMIT 1;"
        observed = comparator.output_db.execute(cmd).fetchall()

        expected = [('Root; d__Bacteria; p__Firmicutes_C; c__Negativicutes; o__Acidaminococcales; f__Acidaminococcaceae', 1, 22.301174490661836, 0)]
        self.assertEqual(observed, expected)
    
    def test_create_compare_table_with_assemblies(self):
        comparator = self.CreateComparator(assemblies_db_path=self.assemblies_db_path)
        comparator.compare()

        cmd = f"SELECT * FROM {comparator.compare_table_name} WHERE assembly=0 LIMIT 1;"
        observed = comparator.output_db.execute(cmd).fetchall()

        expected = [('Root; d__Bacteria; p__Actinobacteriota; c__Acidimicrobiia; o__Acidimicrobiales; f__Bog-793; g__Fen-455; s__Fen-455_sp003139355', 1, 37.77834580878058, 1, 0)]
        self.assertEqual(observed, expected)


if __name__ == "__main__":
    unittest.main()
