#!/usr/bin/env python3

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
        

def main():
    pass


if __name__ == "__main__":
    main()
