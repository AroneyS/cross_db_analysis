############################
### cross_db_analysis.py ###
############################
# Author: Samuel Aroney
# Find prevalent OTUs that are not assembled/binned
# Compare read/assembly/bin SingleM databases

# Manual test code
# python cross_db_analysis.py \
# --reads-db /home/aroneys/projects/01-singleM-coassembly/results/singlem_reads_combined.sdb \
# --bins-db /home/aroneys/projects/01-singleM-coassembly/results/singlem_bins_combined.sdb \
# --output-db /home/aroneys/src/cross_db_analysis/output/output.db \
# --output /home/aroneys/src/cross_db_analysis/output/output.csv

import sqlite3


class SqliteDatabase:
    def __init__(self, db_path):
        self.connection = sqlite3.Connection(db_path)
        self.cursor = sqlite3.Cursor(self.connection)
    
    def execute(self, sql):
        return self.cursor.execute(sql)

class CrossDatabaseComparator:
    def __init__(self, **kwargs):
        self.reads_db_path = kwargs.pop('reads_db')
        self.assemblies_db_path = kwargs.pop('assemblies_db')
        self.bins_db_path = kwargs.pop('bins_db')
        self.output_db_path = kwargs.pop('output_db')
        self.output_path = kwargs.pop('output')

        if self.output_db_path:
            self.output_db = self._connect_database(self.output_db_path)
        else:
            self.output_db = self._connect_database('')
    
    def _connect_database(self, db_path):
        db = SqliteDatabase(db_path)
        return db


def main():
    pass


if __name__ == "__main__":
    main()
