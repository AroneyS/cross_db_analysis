#!/usr/bin/env python3

import unittest, tempfile
from cross_db_analysis import SqliteDatabase


class Test(unittest.TestCase):
    def test_initialise_database(self):
        with tempfile.NamedTemporaryFile(mode='w') as db_path:
            db = SqliteDatabase(db_path.name)
            db.execute("CREATE TABLE test (id integer PRIMARY KEY, name text);")
            db.execute("INSERT INTO test(name) VALUES ('test value');")

            select = db.execute("SELECT * FROM test;").fetchall()
            self.assertEqual(select, [(1, 'test value')])


if __name__ == "__main__":
    unittest.main()
